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
import com.chaiweijian.groupwallet.userservice.util.BadRequestUtil;
import com.chaiweijian.groupwallet.userservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.userservice.util.SimpleUserFormatter;
import com.chaiweijian.groupwallet.userservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.userservice.util.UserAggregateUtil;
import com.chaiweijian.groupwallet.userservice.util.UserStreamValidatorUtil;
import com.chaiweijian.groupwallet.userservice.util.ValidationResult;
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
            var duplicateUidValidationResult = validateDuplicateUid(createUserRequest, resetUidTrackerStream);

            // if duplicate uid validation passed, repartition with userId, so it can be checked for duplication too.
            // uid is generated by identity provider, user id is the string chosen by user
            var partitionByUserId = duplicateUidValidationResult.getPassedStream()
                    .selectKey((key, value) -> value.getUserId())
                    .repartition(Repartitioned.with(Serdes.String(), createUserRequestSerde));

            var userIdValidation = validateUserId(partitionByUserId);

            var duplicateUserIdValidationResult = validateDuplicateUserId(userIdValidation.getPassedStream(), resetUserIdTrackerStream);

            // Format the user before simple validation
            var formattedUser = duplicateUserIdValidationResult.getPassedStream()
                    .mapValues(request -> SimpleUserFormatter.format(
                            request.getUser().toBuilder().setName(String.format("users/%s", request.getUserId())).build()));

            var simpleValidationResult = UserStreamValidatorUtil.validateSimple(formattedUser);

            // emit resetPreviouslySeenUid when:
            // - user id simple validation failed
            // - user id taken validation failed
            // - user simple validation failed
            // the request has been repartitioned to user id so remap the key to uid
            var resetSeenUidOnUserIdTakenValidationFailed = duplicateUserIdValidationResult.getFailedStream()
                    .merge(userIdValidation.getFailedStream())
                    .selectKey((key, value) -> value.getUser().getUid())
                    .mapValues(value -> 0L);
            var resetSeenUidOnSimpleValidationFailed = simpleValidationResult.getFailedStream()
                    .selectKey((key, value) -> value.getUid())
                    .mapValues(value -> 0L);
            resetSeenUidOnUserIdTakenValidationFailed.merge(resetSeenUidOnSimpleValidationFailed)
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.Long()))
                    .to("groupwallet.userservice.resetPreviouslySeenUid", Produced.with(Serdes.String(), Serdes.Long()));

            // emit resetPreviouslySeenUserId when simple validation failed
            // SimpleValidationFailed stream has user id as key, no need to repartition
            simpleValidationResult.getFailedStream().mapValues(value -> 0L)
                    .to("groupwallet.userservice.resetPreviouslySeenUserId", Produced.with(Serdes.String(), Serdes.Long()));

            // at this stage the request has passed all validation, create a new user now
            var newUser = simpleValidationResult.getPassedStream()
                    .mapValues(value -> value.toBuilder()
                            .setAggregateVersion(1)
                            .setEtag(UserAggregateUtil.calculateEtag(value.getName(), 1))
                            .build());

            // dispatch user created event
            // the newUser stream was partition by user_id, repartition with name
            newUser
                    .selectKey((key, value) -> value.getName())
                    .repartition(Repartitioned.with(Serdes.String(), userSerde))
                    .to("groupwallet.userservice.UserCreated-events", Produced.with(Serdes.String(), userSerde));

            var successStatus = newUser.mapValues(value -> OkStatusUtil.packStatus(value, "User created."));

            // Merge all the possible outcomes of a create user request
            return duplicateUidValidationResult.getStatusStream()
                    .merge(userIdValidation.getStatusStream())
                    .merge(duplicateUserIdValidationResult.getStatusStream())
                    .merge(simpleValidationResult.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, CreateUserRequest> validateDuplicateUid(KStream<String, CreateUserRequest> input, KStream<String, Long> resetUidTrackerStream) {
        // keep track of seen uid to detect duplicate uid
        // key: uid, value: count of previously seen uid
        var seenUidTracker = input
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
        var validation = input
                .leftJoin(seenUidTable,
                        // if the count is not null (first time seen) or 0 (reset after validation failed),
                        // it is duplicate uid
                        (request, count) -> new ValidationResult<>(request).setPass(count == null || count == 0));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        // The error status stream when uid is already registered
        // key: uid, value: Status with details on uid already registered
        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.ALREADY_EXISTS_VALUE)
                        .setMessage("User already exists.")
                        .addDetails(Any.pack(
                                ErrorInfo.newBuilder()
                                        .setReason(String.format("User with uid %s already exists.", value.getUser().getUid()))
                                        .setDomain("userservice.groupwallet.chaiweijian.com")
                                        .putMetadata("uid", value.getUser().getUid())
                                        .build()
                        )).build());

        // The stream branch that passed duplicate uid validation
        // key: uid, value: CreateUserRequest
        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    public static StreamValidationResult<String, CreateUserRequest> validateUserId(KStream<String, CreateUserRequest> input) {
        var validation = input
                .mapValues(SimpleUserIdValidator::validate);

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(SimpleUserIdValidator::getRequest);

        var status = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(SimpleUserIdValidator::getRequest);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, CreateUserRequest> validateDuplicateUserId(KStream<String, CreateUserRequest> input, KStream<String, Long> resetUserIdTrackerStream) {
        // keep track of seen user id to detect duplicate user id
        // key: user id, value: count of previously seen uid
        var seenUserIdTracker = input
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
        var validation = input
                .leftJoin(seenUserIdTable,
                        // if the count is not null (first time seen) or 0 (reset after validation failed),
                        // it is duplicate user id
                        (request, count) -> new ValidationResult<>(request).setPass(count == null || count == 0));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        // The error status stream when user id is already taken
        // key: user id, value: Status with details on user id is taken
        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.ALREADY_EXISTS_VALUE)
                        .setMessage("User Id is taken.")
                        .addDetails(Any.pack(
                                ErrorInfo.newBuilder()
                                        .setReason(String.format("User with user id %s already exists.", value.getUserId()))
                                        .setDomain("userservice.groupwallet.chaiweijian.com")
                                        .putMetadata("userId", value.getUserId())
                                        .build()
                        )).build());

        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    @Data
    private static class SimpleUserIdValidator implements SimpleValidator {
        private final CreateUserRequest request;
        private final BadRequest badRequest;
        private final boolean failed;

        public static SimpleUserIdValidator validate(CreateUserRequest createUserRequest) {
            var builder = BadRequest.newBuilder();

            validateUserId(builder, createUserRequest.getUserId());

            return new SimpleUserIdValidator(createUserRequest, builder.build(), builder.getFieldViolationsCount() > 0);
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
