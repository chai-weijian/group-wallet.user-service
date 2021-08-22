// Copyright 2021 Chai Wei Jian
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.chaiweijian.groupwallet.userservice.update;

import com.chaiweijian.groupwallet.userservice.interfaces.SimpleValidator;
import com.chaiweijian.groupwallet.userservice.util.*;
import com.chaiweijian.groupwallet.userservice.v1.UpdateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.protobuf.Any;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.rpc.*;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

@Component
public class UpdateUserRequestProcessor {
    private final KafkaProtobufSerde<User> userSerde;

    public UpdateUserRequestProcessor(KafkaProtobufSerde<User> userSerde) {
        this.userSerde = userSerde;
    }

    @Bean
    public BiFunction<KStream<String, UpdateUserRequest>, GlobalKTable<String, User>, KStream<String, Status>> updateUser() {
        return (updateUserRequest, userAggregateStore) -> {

            var userExistsValidation = updateUserRequest
                    .leftJoin(userAggregateStore,
                            (leftKey, leftValue) -> leftKey,
                            UpdateRequestAndExistingUser::new);

            var userNotExistsErrorStatus = userExistsValidation
                    .filterNot((key, value) -> value.currentUserExists())
                    .mapValues(value -> Status.newBuilder()
                            .setCode(Code.NOT_FOUND_VALUE)
                            .setMessage(String.format("User with name %s does not exists.", value.getRequest().getUser().getName()))
                            .build());

            var userExistsValidationPassed = userExistsValidation
                    .filter(((key, value) -> value.currentUserExists()));

            var etagValidation = userExistsValidationPassed
                    .mapValues(value -> new RequestValidation<>(value, !value.getRequest().getUser().getEtag().equals(value.getCurrentUser().getEtag())));

            var etagErrorStatus = etagValidation
                    .filter((key, value) -> value.isFailed())
                    .mapValues(RequestValidation::getRequest)
                    .mapValues(value -> Status.newBuilder()
                            .setCode(Code.ABORTED_VALUE)
                            .setMessage("Concurrency error.")
                            .addDetails(Any.pack(ErrorInfo.newBuilder()
                                    .setReason("Etag is not the latest version.")
                                    .setDomain("userservice.groupwallet.chaiweijian.com")
                                    .putMetadata("providedEtag", value.getRequest().getUser().getEtag())
                                    .build()))
                            .build());

            var etagValidationPassed = etagValidation
                    .filterNot((key, value) -> value.isFailed())
                    .mapValues(RequestValidation::getRequest);

            var fieldMaskValidation = etagValidationPassed
                    .mapValues(FieldMaskValidator::validate);

            var fieldMaskErrorStatus = fieldMaskValidation
                    .filter(((key, value) -> value.isFailed()))
                    .mapValues(BadRequestUtil::packStatus);

            var fieldMaskPassed = fieldMaskValidation
                    .filterNot(((key, value) -> value.isFailed()))
                    .mapValues(FieldMaskValidator::getRequestAndExistingUser);

            var mergedUser = fieldMaskPassed
                    .mapValues(value -> {
                        var result = value.getCurrentUser().toBuilder();
                        FieldMaskUtil.merge(value.getRequest().getUpdateMask(), value.getRequest().getUser(), result);
                        return SimpleUserFormatter.format(result.build());
                    });

            var simpleValidation = mergedUser
                    .mapValues((ValueMapper<User, SimpleUserValidator>) SimpleUserValidator::validate);

            var simpleValidationFailed = simpleValidation
                    .filter(((key, value) -> value.isFailed()));

            var simpleValidationErrorStatus = simpleValidationFailed
                    .mapValues(BadRequestUtil::packStatus);

            var simpleValidationPassed = simpleValidation
                    .filterNot(((key, value) -> value.isFailed()));

            var updatedUser = simpleValidationPassed
                    .mapValues(SimpleUserValidator::getUser)
                    .mapValues(value -> value.toBuilder()
                            .setAggregateVersion(value.getAggregateVersion() + 1)
                            .setEtag(UserAggregateUtil.calculateEtag(value.getAggregateVersion() + 1))
                            .build());

            updatedUser.to("groupwallet.userservice.UserUpdated-events", Produced.with(Serdes.String(), userSerde));

            var successStatus = updatedUser.mapValues(value -> Status.newBuilder()
                    .setCode(Code.OK_VALUE)
                    .setMessage("User successfully created.")
                    .addDetails(Any.pack(value))
                    .build());

            return userNotExistsErrorStatus
                    .merge(etagErrorStatus)
                    .merge(fieldMaskErrorStatus)
                    .merge(simpleValidationErrorStatus)
                    .merge(successStatus);
        };
    }

    @Data
    private static class FieldMaskValidator implements SimpleValidator {
        private final UpdateRequestAndExistingUser requestAndExistingUser;
        private final BadRequest badRequest;
        private final boolean failed;

        public static FieldMaskValidator validate(UpdateRequestAndExistingUser requestAndExistingUser) {
            var badRequest = BadRequest.newBuilder();

            validateFieldMask(badRequest, requestAndExistingUser);
            validateImmutableFieldMask(badRequest, requestAndExistingUser);

            return new FieldMaskValidator(requestAndExistingUser, badRequest.build(), badRequest.getFieldViolationsCount() > 0);
        }

        private static void validateFieldMask(BadRequest.Builder builder, UpdateRequestAndExistingUser requestAndExistingUser) {
            if (!FieldMaskUtil.isValid(User.class, requestAndExistingUser.getRequest().getUpdateMask())) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("update_mask").setDescription("Unable to map update_mask to User type.").build());
            }
        }

        private static void validateImmutableFieldMask(BadRequest.Builder builder, UpdateRequestAndExistingUser requestAndExistingUser) {
            if (requestAndExistingUser.getRequest().getUpdateMask().getPathsList().contains("uid")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("update_mask").setDescription("Uid cannot be changed.").build());
            }
            if (requestAndExistingUser.getRequest().getUpdateMask().getPathsList().contains("name")) {
                builder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("update_mask").setDescription("Name cannot be changed.").build());
            }
        }
    }

    @Data
    private static class UpdateRequestAndExistingUser {
        private final UpdateUserRequest request;
        private final User currentUser;

        public boolean currentUserExists() {
            return currentUser != null;
        }
    }
}
