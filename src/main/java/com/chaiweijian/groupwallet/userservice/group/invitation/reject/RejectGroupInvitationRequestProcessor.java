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

package com.chaiweijian.groupwallet.userservice.group.invitation.reject;

import com.chaiweijian.groupwallet.userservice.util.BadRequestUtil;
import com.chaiweijian.groupwallet.userservice.util.FailedPreconditionUtil;
import com.chaiweijian.groupwallet.userservice.util.GroupInvitationUtil;
import com.chaiweijian.groupwallet.userservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.userservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.userservice.util.ValidationResult;
import com.chaiweijian.groupwallet.userservice.v1.RejectGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

@Component
public class RejectGroupInvitationRequestProcessor {

    private final KafkaProtobufSerde<GroupInvitation> groupInvitationSerde;

    public RejectGroupInvitationRequestProcessor(KafkaProtobufSerde<GroupInvitation> groupInvitationSerde) {
        this.groupInvitationSerde = groupInvitationSerde;
    }

    @Bean
    public BiFunction<KStream<String, RejectGroupInvitationRequest>, GlobalKTable<String, GroupInvitation>, KStream<String, Status>> rejectGroupInvitation() {
        return (rejectGroupInvitationRequest, groupInvitationStore) -> {

            var joined = rejectGroupInvitationRequest.leftJoin(groupInvitationStore,
                            (leftKey, leftValue) -> leftKey,
                            RequestAndGroupInvitation::new);

            var groupInvitationExistsValidation = validateGroupInvitationExists(joined);

            var groupInvitationEtagValidation = validateGroupInvitationEtag(groupInvitationExistsValidation.getPassedStream());

            var groupInvitationStateValidation = validateGroupInvitationState(
                    groupInvitationEtagValidation.getPassedStream().mapValues(RequestAndGroupInvitation::getGroupInvitation));

            var rejected = groupInvitationStateValidation.getPassedStream()
                    .mapValues(value -> value.toBuilder()
                            .setUpdateTime(fromMillis(currentTimeMillis()))
                            .setAggregateVersion(value.getAggregateVersion() + 1)
                            .setEtag(GroupInvitationUtil.calculateEtag(value.getName(), value.getAggregateVersion() + 1))
                            .setState(GroupInvitation.State.REJECTED)
                            .build());

            rejected.to("groupwallet.userservice.GroupInvitationRejected-events", Produced.with(Serdes.String(), groupInvitationSerde));

            var successStatus = rejected.mapValues(value -> OkStatusUtil.packStatus(value, "Group Invitation rejected"));

            return groupInvitationExistsValidation.getStatusStream()
                    .merge(groupInvitationEtagValidation.getStatusStream())
                    .merge(groupInvitationStateValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, RequestAndGroupInvitation> validateGroupInvitationExists(KStream<String, RequestAndGroupInvitation> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setFail(value.getGroupInvitation() == null));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("group")
                                .setDescription(String.format("%s does not exists.", value.getRequest().getGroupInvitation().getName()))
                                .build())
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, RequestAndGroupInvitation> validateGroupInvitationEtag(KStream<String, RequestAndGroupInvitation> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getRequest().getGroupInvitation().getEtag().equals(value.getGroupInvitation().getEtag())));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.ABORTED_VALUE)
                        .setMessage("Concurrency error.")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("Etag is not the latest version.")
                                .setDomain("userservice.groupwallet.chaiweijian.com")
                                .putMetadata("providedEtag", value.getRequest().getGroupInvitation().getEtag())
                                .build()))
                        .build());

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, GroupInvitation> validateGroupInvitationState(KStream<String, GroupInvitation> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getState() == GroupInvitation.State.PENDING));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value ->
                        PreconditionFailure.newBuilder()
                                        .addViolations(PreconditionFailure.Violation.newBuilder()
                                                .setType("NOT_PENDING")
                                                .setSubject("userservice.groupwallet.chaiweijian.com/GroupInvitation")
                                                .setDescription("Group invitation is not in pending state.")).build())
                .mapValues(FailedPreconditionUtil::packStatus);

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    @Data
    private static class RequestAndGroupInvitation{
        private final RejectGroupInvitationRequest request;
        private final GroupInvitation groupInvitation;
    }
}
