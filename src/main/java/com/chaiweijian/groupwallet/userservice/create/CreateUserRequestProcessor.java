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

import com.chaiweijian.groupwallet.userservice.v1.CreateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.chaiweijian.groupwallet.userservice.v1.UserCreated;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
    private final KafkaProtobufSerde<UserCreated> userCreatedSerde;

    public CreateUserRequestProcessor(KafkaProtobufSerde<CreateUserRequest> createUserRequestSerde, KafkaProtobufSerde<UserCreated> userCreatedSerde) {
        this.createUserRequestSerde = createUserRequestSerde;
        this.userCreatedSerde = userCreatedSerde;
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
                    .merge(resetUidTrackerStream)
                    .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            // check whether uid has been registered
            // key: uid, value: request validation (fail if uid has previously seen and didn't get reset)
            var duplicateUidValidation = createUserRequest
                    .leftJoin(seenUidTable,
                            // consider duplicate only when count > 1. If count is 1, that's the current request
                            (request, count) -> new RequestValidation(request, (count == null ? 0L : count) > 1));

            // The error status stream when uid is already registered
            // key: uid, value: Status with details on uid already registered
            var duplicateUidErrorStatus = duplicateUidValidation
                    .filter(((key, value) -> value.isFailed()))
                    .mapValues(value -> Status.newBuilder()
                            .setCode(6)
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
                    .mapValues(RequestValidation::getRequest)
                    .mapValues(request -> request.toBuilder().setUser(SimpleFormatter.format(request.getUser())).build());

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
                    .merge(resetUserIdTrackerStream)
                    .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

            // check whether user id is taken
            // key: user id, value: request validation (fail if user id has previously seen and didn't get reset)
            var userIdTakenValidation = partitionByUserId
                    .leftJoin(seenUserIdTable,
                            // consider duplicate only when count > 1. If count is 1, that's the current request
                            (request, count) -> new RequestValidation(request, (count == null ? 0L : count) > 1));

            var userIdTakenValidationFailed = userIdTakenValidation
                    .filter(((key, value) -> value.isFailed()));

            // The error status stream when user id is already taken
            // key: user id, value: Status with details on user id is taken
            var userIdTakenErrorStatus = userIdTakenValidationFailed
                    .mapValues(value -> Status.newBuilder()
                            .setCode(6)
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

            var simpleValidation = userIdTakenValidationPassed
                    .mapValues(SimpleValidator::validate);

            var simpleValidationFailed = simpleValidation
                    .filter(((key, value) -> value.isFailed()));

            var simpleValidationErrorStatus = simpleValidationFailed
                    .mapValues(value -> Status.newBuilder()
                            .setCode(3)
                            .setMessage("Request contain invalid arguments. See details for more information.")
                            .addDetails(Any.pack(value.getBadRequest()))
                            .build());

            var simpleValidationPassed = simpleValidation
                    .filterNot(((key, value) -> value.isFailed()))
                    .mapValues(SimpleValidator::getRequest);

            // emit resetPreviouslySeenUid when user id taken validation and simple validation failed
            // the request has been repartitioned to user id so remap the key to uid
            var resetSeenUidOnUserIdTakenValidationFailed = userIdTakenValidationFailed
                    .map((key, value) -> KeyValue.pair(value.getRequest().getUser().getUid(), 0L));
            var resetSeenUidOnSimpleValidationFailed = simpleValidationFailed
                    .map((key, value) -> KeyValue.pair(value.getRequest().getUser().getUid(), 0L));
            resetSeenUidOnUserIdTakenValidationFailed.merge(resetSeenUidOnSimpleValidationFailed)
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

            newUser
                    .mapValues(value -> UserCreated.newBuilder().setUser(value).build())
                    .selectKey((key, value) -> value.getUser().getName())
                    .repartition(Repartitioned.with(Serdes.String(), userCreatedSerde))
                    .to("groupwallet.userservice.UserCreated-events", Produced.with(Serdes.String(), userCreatedSerde));

            var successStatus = newUser.mapValues(value -> Status.newBuilder()
                    .setCode(0)
                    .setMessage("User successfully created.")
                    .addDetails(Any.pack(value))
                    .build());

            // Merge all the possible outcomes of a create user request
            return duplicateUidErrorStatus.merge(userIdTakenErrorStatus).merge(simpleValidationErrorStatus).merge(successStatus);
        };
    }

    private static final class SimpleFormatter {
        public static User format(User user) {
            return user.toBuilder()
                    .setDisplayName(user.getDisplayName().trim())
                    .build();
        }
    }

    @Data
    private static final class SimpleValidator {
        private final CreateUserRequest request;
        private final BadRequest badRequest;
        private final boolean failed;

        private SimpleValidator(CreateUserRequest request, BadRequest badRequest, boolean failed) {
            this.request = request;
            this.badRequest = badRequest;
            this.failed = failed;
        }

        public static SimpleValidator validate(CreateUserRequest request) {
            var builder = BadRequest.newBuilder();

            validateUserId(builder, request);
            validateDisplayName(builder, request);
            validateEmail(builder, request);
            validateUid(builder, request);

            return new SimpleValidator(request, builder.build(), builder.getFieldViolationsCount() > 0);
        }

        private static void validateUserId(BadRequest.Builder badRequestBuilder, CreateUserRequest request) {
            var userId = request.getUserId();

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

        private static void validateDisplayName(BadRequest.Builder badRequestBuilder, CreateUserRequest request) {
            var displayName = request.getUser().getDisplayName();

            if (displayName.length() == 0) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must not be empty."));
            } else if (displayName.length() < 4) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must has minimum 4 characters."));
            } else if (displayName.length() > 120) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must has maximum 120 characters."));
            }
        }

        private static void validateEmail(BadRequest.Builder badRequestBuilder, CreateUserRequest request) {
            var email = request.getUser().getEmail();

            if (email.length() == 0) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.email").setDescription("User email must not be empty."));
            }
        }

        private static void validateUid(BadRequest.Builder badRequestBuilder, CreateUserRequest request) {
            var uid = request.getUser().getUid();

            if (uid.length() == 0) {
                badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.uid").setDescription("User uid must not be empty."));
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static final class RequestValidation {
        private final CreateUserRequest request;
        // Use fail instead of pass, since validation
        // usually check against some precondition
        // is not met, rather than check against all
        // precondition met
        private final boolean failed;
    }
}
