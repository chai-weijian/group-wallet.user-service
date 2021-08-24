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

package com.chaiweijian.groupwallet.userservice.group.invitation.create;

import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.userservice.util.BadRequestUtil;
import com.chaiweijian.groupwallet.userservice.util.GroupInvitationUtil;
import com.chaiweijian.groupwallet.userservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.userservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.userservice.util.ValidationResult;
import com.chaiweijian.groupwallet.userservice.v1.CreateGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.rpc.BadRequest;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.function.Function;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

@Component
public class CreateGroupInvitationRequestProcessor {

    private final KafkaProtobufSerde<GroupInvitation> groupInvitationSerde;

    public CreateGroupInvitationRequestProcessor(KafkaProtobufSerde<GroupInvitation> groupInvitationSerde) {
        this.groupInvitationSerde = groupInvitationSerde;
    }

    @Bean
    public Function<KStream<String, CreateGroupInvitationRequest>, Function<GlobalKTable<String, User>, Function<GlobalKTable<String, Group>, KStream<String, Status>>>> createGroupInvitation() {
        return createGroupInvitationRequest -> userAggregateStore -> groupAggregateStore -> {
            var groupValidation = validateGroup(createGroupInvitationRequest, groupAggregateStore);

            var parentValidation = validateParent(groupValidation.getPassedStream(), userAggregateStore);

            var newGroupInvitation = parentValidation.getPassedStream()
                    .mapValues(value -> value.getGroupInvitation().toBuilder()
                            .setName(String.format("%s/groupInvitations/%s", value.getParent(), UUID.randomUUID()))
                            .setAggregateVersion(1)
                            .setEtag(GroupInvitationUtil.calculateEtag(value.getGroupInvitation().getName(), 1))
                            .setState(GroupInvitation.State.PENDING)
                            .setCreateTime(fromMillis(currentTimeMillis()))
                            .build());

            newGroupInvitation
                    .selectKey((key, value) -> value.getName())
                    .repartition(Repartitioned.with(Serdes.String(), groupInvitationSerde))
                    .to("groupwallet.userservice.GroupInvitationCreated-events", Produced.with(Serdes.String(), groupInvitationSerde));

            var successStatus = newGroupInvitation
                    .mapValues(value -> OkStatusUtil.packStatus(value, "Group Invitation created."));

            return groupValidation.getStatusStream()
                    .merge(parentValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, CreateGroupInvitationRequest> validateGroup(KStream<String, CreateGroupInvitationRequest> input, GlobalKTable<String, Group> groupAggregateStore) {
        var validation = input
                .leftJoin(groupAggregateStore,
                        (leftKey, leftValue) -> leftValue.getGroupInvitation().getGroup(),
                        (leftValue, rightValue) -> new ValidationResult<>(leftValue).setFail(rightValue == null));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("group")
                                .setDescription(String.format("%s does not exists.", value.getGroupInvitation().getGroup()))
                                .build())
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, CreateGroupInvitationRequest> validateParent(KStream<String, CreateGroupInvitationRequest> input, GlobalKTable<String, User> userAggregateStore) {
        var validation = input
                .leftJoin(userAggregateStore,
                        (leftKey, leftValue) -> leftKey,
                        (leftValue, rightValue) -> new ValidationResult<>(leftValue).setFail(rightValue == null));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("parent")
                                .setDescription(String.format("%s does not exists.", value.getParent()))
                                .build())
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }
}
