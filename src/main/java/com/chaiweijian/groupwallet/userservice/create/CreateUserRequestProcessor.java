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

package com.chaiweijian.groupwallet.userservice.create;

import com.chaiweijian.groupwallet.userservice.interfaces.SimpleValidator;
import com.chaiweijian.groupwallet.userservice.util.*;
import com.chaiweijian.groupwallet.userservice.v1.CreateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;
import java.util.regex.Pattern;

@Component
public class CreateUserRequestProcessor {

    private final KafkaProtobufSerde<CreateUserRequest> createUserRequestSerde;
    private final KafkaProtobufSerde<User> userSerde;

    public CreateUserRequestProcessor(KafkaProtobufSerde<CreateUserRequest> createUserRequestSerde, KafkaProtobufSerde<User> userSerde) {
        this.createUserRequestSerde = createUserRequestSerde;
        this.userSerde = userSerde;
    }

    // handle create user request
    //
    // The request go through a few steps of validation
    // 1. check if uid already exists
    // 2. check if user id is taken
    // 3. simple validation on field format
    //
    // Once all the validation passed, this processor will create the user
    // and dispatch UserCreated event.
    // If the validation failed, it must allow the user request with same uid
    // and user id to try again.
    @Bean
    public Function<KStream<String, CreateUserRequest>, Function<KStream<String, Long>, Function<KStream<String, Long>, KStream<String, Status>>>> createUser() {
        return createUserRequest -> resetUidTrackerStream -> resetUserIdTrackerStream -> {
            // keep track of seen uid to detect duplicate uid
            // key: uid, value: count of previously seen uid
            var seenUidTracker = createUserRequest
                    .groupByKey()
                    .count()
                    .toStream();

            // KTable that contain information about seen uid
            // seenUidTracker above will dutifully track each uid has been seen
            // how many times, but it cannot tell when a validation error occur and
            // the uid is not registered.
            //
            // The resetUidTrackerStream done exactly that, when the request
            // get validation error downstream, resetUidTrackerStream will emit to
            // reset the uid in this table to 0
            var seenUidTable = seenUidTracker
                    .merge(resetUidTrackerStream.filter((key, value) -> value != null))
                    .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            // check whether uid has been registered
            // key: uid, value: request validation (fail if uid has previously seen and didn't get reset)
            var duplicateUidValidation = createUserRequest
                    .leftJoin(seenUidTable,
                            // if the count is not null (first time seen) or 0 (reset after validation failed),
                            // it is duplicate uid
                            (request, count) -> new RequestValidation<>(request, !(count == null || count == 0)));

            // The error status stream when uid is already registered
            // key: uid, value: Status with details on uid already registered
            var duplicateUidErrorStatus = duplicateUidValidation
                    .filter(((key, value) -> value.isFailed()))
                    .mapValues(value -> Status.newBuilder()
                            .setCode(Code.ALREADY_EXISTS_VALUE)
                            .setMessage("User already exists.")
                            .addDetails(Any.pack(
                                    ErrorInfo.newBuilder()
                                            .setReason(String.format("User with uid %s already exists.", value.getRequest().getUser().getUid()))
                                            .setDomain("userservice.groupwallet.chaiweijian.com")
                                            .putMetadata("uid", value.getRequest().getUser().getUid())
                                            .build()
                            )).build());

            // The stream branch that passed duplicate uid validation
            // key: uid, value: CreateUserRequest
            var passedDuplicateUidValidation = duplicateUidValidation
                    .filterNot(((key, value) -> value.isFailed()))
                    .mapValues(RequestValidation::getRequest);

            // now that uid is unique, next need to check user id is also unique
            // uid is generated by identity provider, user id is the string chosen by user
            var partitionByUserId = passedDuplicateUidValidation
                    .selectKey((key, value) -> value.getUserId())
                    .repartition(Repartitioned.with(Serdes.String(), createUserRequestSerde));

            // keep track of seen user id to detect duplicate user id
            // key: user id, value: count of previously seen uid
            var seenUserIdTracker = partitionByUserId
                    .groupByKey()
                    .count()
                    .toStream();

            // KTable that contain information about seen user id
            // seenUserIdTracker above will dutifully track each user id has been seen
            // how many times, but it cannot tell when a validation error occur and
            // the user id is not registered.
            //
            // The resetUserIdTrackerStream done exactly that, when the request
            // get validation error downstream, resetUserIdTrackerStream will emit to
            // reset the user id in this table to 0
            var seenUserIdTable = seenUserIdTracker
                    .merge(resetUserIdTrackerStream.filter(((key, value) -> value != null)))
                    .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            // check whether user id is taken
            // key: user id, value: request validation (fail if user id has previously seen and didn't get reset)
            var userIdTakenValidation = partitionByUserId
                    .leftJoin(seenUserIdTable,
                            // if the count is not null (first time seen) or 0 (reset after validation failed),
                            // it is duplicate user id
                            (request, count) -> new RequestValidation<>(request, !(count == null || count == 0)));

            var userIdTakenValidationFailed = userIdTakenValidation
                    .filter(((key, value) -> value.isFailed()));

            // The error status stream when user id is already taken
            // key: user id, value: Status with details on user id is taken
            var userIdTakenErrorStatus = userIdTakenValidationFailed
                    .mapValues(value -> Status.newBuilder()
                            .setCode(Code.ALREADY_EXISTS_VALUE)
                            .setMessage("User Id is taken.")
                            .addDetails(Any.pack(
                                    ErrorInfo.newBuilder()
                                            .setReason(String.format("User with user id %s already exists.", value.getRequest().getUserId()))
                                            .setDomain("userservice.groupwallet.chaiweijian.com")
                                            .putMetadata("userId", value.getRequest().getUserId())
                                            .build()
                            )).build());

            var userIdTakenValidationPassed = userIdTakenValidation
                    .filterNot(((key, value) -> value.isFailed()))
                    .mapValues(RequestValidation::getRequest);

            var formattedUser = userIdTakenValidationPassed
                    .mapValues(request -> request.toBuilder().setUser(SimpleUserFormatter.format(request.getUser())).build());

            var simpleValidation = formattedUser
                    .mapValues(SimpleCreateUserRequestValidator::validate);

            var simpleValidationFailed = simpleValidation
                    .filter(((key, value) -> value.isFailed()));

            var simpleValidationErrorStatus = simpleValidationFailed
                    .mapValues(BadRequestUtil::packStatus);

            var simpleValidationPassed = simpleValidation
                    .filterNot(((key, value) -> value.isFailed()))
                    .mapValues(SimpleCreateUserRequestValidator::getRequest);

            // emit resetPreviouslySeenUid when user id taken validation and simple validation failed
            // the request has been repartitioned to user id so remap the key to uid
            var resetSeenUidOnUserIdTakenValidationFailed = userIdTakenValidationFailed
                    .selectKey((key, value) -> value.getRequest().getUser().getUid())
                    .mapValues(value -> 0L);
            var resetSeenUidOnSimpleValidationFailed = simpleValidationFailed
                    .selectKey((key, value) -> value.getRequest().getUser().getUid())
                    .mapValues(value -> 0L);
            resetSeenUidOnUserIdTakenValidationFailed.merge(resetSeenUidOnSimpleValidationFailed)
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.Long()))
                    .to("groupwallet.userservice.resetPreviouslySeenUid", Produced.with(Serdes.String(), Serdes.Long()));

            // emit resetPreviouslySeenUserId when simple validation failed
            // SimpleValidationFailed stream has user id as key, no need to repartition
            simpleValidationFailed.mapValues(value -> 0L)
                    .to("groupwallet.userservice.resetPreviouslySeenUserId", Produced.with(Serdes.String(), Serdes.Long()));

            // at this stage the request has passed all validation, it is safe to create a new user now
            var newUser = simpleValidationPassed
                    .mapValues(value -> value.getUser().toBuilder()
                            .setName(String.format("users/%s", value.getUserId()))
                            .build());

            // dispatch user created event
            // the newUser stream was partition by user_id, repartition with name
            newUser
                    .selectKey((key, value) -> value.getName())
                    .repartition(Repartitioned.with(Serdes.String(), userSerde))
                    .to("groupwallet.userservice.UserCreated-events", Produced.with(Serdes.String(), userSerde));

            var successStatus = newUser.mapValues(value -> Status.newBuilder()
                    .setCode(Code.OK_VALUE)
                    .setMessage("User successfully created.")
                    .addDetails(Any.pack(value.toBuilder().setAggregateVersion(1).setEtag(UserAggregateUtil.calculateEtag(1)).build()))
                    .build());

            // Merge all the possible outcomes of a create user request
            return duplicateUidErrorStatus.merge(userIdTakenErrorStatus).merge(simpleValidationErrorStatus).merge(successStatus);
        };
    }

    @Data
    private static class SimpleCreateUserRequestValidator implements SimpleValidator {
        private final CreateUserRequest request;
        private final BadRequest badRequest;
        private final boolean failed;

        public static SimpleCreateUserRequestValidator validate(CreateUserRequest createUserRequest) {
            var builder = BadRequest.newBuilder();

            validateUserId(builder, createUserRequest.getUserId());
            SimpleUserValidator.validate(builder, createUserRequest.getUser());

            return new SimpleCreateUserRequestValidator(createUserRequest, builder.build(), builder.getFieldViolationsCount() > 0);
        }

        public static void validateUserId(BadRequest.Builder badRequestBuilder, String userId) {
            if (userId.length() == 0) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("userId").setDescription("User Id must not be empty."));
            } else if (userId.length() < 4) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("userId").setDescription("User Id must has minimum 4 characters."));
            } else if (userId.length() > 63) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("userId").setDescription("User Id must has maximum 63 characters."));
            }

            if (Pattern.compile("[^a-z0-9-]").matcher(userId).find()) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("userId").setDescription("User Id can only contain lower case letter, digit, and hyphen."));
            }
        }
    }
}
