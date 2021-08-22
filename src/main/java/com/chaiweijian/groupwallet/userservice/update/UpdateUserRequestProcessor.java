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

import com.chaiweijian.groupwallet.userservice.util.*;
import com.chaiweijian.groupwallet.userservice.v1.UpdateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.rpc.BadRequest;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
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

            var userExistsValidationResult = validateUserExists(updateUserRequest, userAggregateStore);

            var updateRequestAndExistingUser = userExistsValidationResult.getPassedStream()
                    .leftJoin(userAggregateStore,
                            (leftKey, leftValue) -> leftKey,
                            UpdateRequestAndExistingUser::new);

            var etagValidationResult = validateEtag(updateRequestAndExistingUser);

            var updatedFieldMask = etagValidationResult.getPassedStream()
                    .mapValues(value -> new UpdateRequestAndExistingUser(
                            value.getRequest().toBuilder().setUpdateMask(removeImmutableField(value.getRequest().getUpdateMask())).build(),
                            value.getCurrentUser()));

            var fieldMaskValidationResult = validateFieldMask(updatedFieldMask);

            var mergedUser = fieldMaskValidationResult.getPassedStream()
                    .mapValues(value -> {
                        var result = value.getCurrentUser().toBuilder();
                        FieldMaskUtil.merge(value.getRequest().getUpdateMask(), value.getRequest().getUser(), result);
                        return SimpleUserFormatter.format(result.build());
                    });

            var simpleValidation = validateSimple(mergedUser);

            var updatedUser = simpleValidation.getPassedStream()
                    .mapValues(value -> value.toBuilder()
                            .setAggregateVersion(value.getAggregateVersion() + 1)
                            .setEtag(UserAggregateUtil.calculateEtag(value.getAggregateVersion() + 1))
                            .build());

            updatedUser.to("groupwallet.userservice.UserUpdated-events", Produced.with(Serdes.String(), userSerde));

            var successStatus = updatedUser.mapValues(value -> OkStatusUtil.packStatus(value, "User updated."));

            return userExistsValidationResult.getStatusStream()
                    .merge(etagValidationResult.getStatusStream())
                    .merge(fieldMaskValidationResult.getStatusStream())
                    .merge(simpleValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, UpdateUserRequest> validateUserExists(KStream<String, UpdateUserRequest> input, GlobalKTable<String, User> userAggregateStore) {
        var validation = input
                .leftJoin(userAggregateStore,
                        (leftKey, leftValue) -> leftKey,
                        UpdateRequestAndExistingUser::new);

        var failed = validation
                .filterNot((key, value) -> value.currentUserExists())
                .mapValues(UpdateRequestAndExistingUser::getRequest);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("User with name %s does not exists.", value.getUser().getName()))
                        .build());

        var passed = validation
                .filter(((key, value) -> value.currentUserExists()))
                .mapValues(UpdateRequestAndExistingUser::getRequest);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, UpdateRequestAndExistingUser> validateEtag(KStream<String, UpdateRequestAndExistingUser> input) {
        var etagValidation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getRequest().getUser().getEtag().equals(value.getCurrentUser().getEtag())));

        var failed = etagValidation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.ABORTED_VALUE)
                        .setMessage("Concurrency error.")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("Etag is not the latest version.")
                                .setDomain("userservice.groupwallet.chaiweijian.com")
                                .putMetadata("providedEtag", value.getRequest().getUser().getEtag())
                                .build()))
                        .build());

        var passed = etagValidation
                .filterNot((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, User> validateSimple(KStream<String, User> input) {
        var validation = input
                .mapValues((ValueMapper<User, SimpleUserValidator>) SimpleUserValidator::validate);

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(SimpleUserValidator::getUser);

        var status = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(SimpleUserValidator::getUser);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private static FieldMask removeImmutableField(FieldMask fieldMask) {
        final FieldMask OUTPUT_ONLY = FieldMask.newBuilder()
                .addPaths("uid")
                .addPaths("name")
                .addPaths("aggregate_version")
                .addPaths("etag")
                .build();

        return FieldMaskUtil.subtract(fieldMask, OUTPUT_ONLY);
    }

    private static StreamValidationResult<String, UpdateRequestAndExistingUser> validateFieldMask(KStream<String, UpdateRequestAndExistingUser> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(FieldMaskUtil.isValid(User.class, value.getRequest().getUpdateMask())));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("update_mask")
                                .setDescription("Unable to map update_mask to User type."))
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    @Data
    public static class UpdateRequestAndExistingUser {
        private final UpdateUserRequest request;
        private final User currentUser;

        public boolean currentUserExists() {
            return currentUser != null;
        }
    }
}
