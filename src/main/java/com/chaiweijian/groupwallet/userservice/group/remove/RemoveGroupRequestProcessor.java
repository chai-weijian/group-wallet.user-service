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

package com.chaiweijian.groupwallet.userservice.group.remove;

import com.chaiweijian.groupwallet.userservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.userservice.util.RequestAndExistingUser;
import com.chaiweijian.groupwallet.userservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.userservice.util.UserAggregateUtil;
import com.chaiweijian.groupwallet.userservice.util.ValidationResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.chaiweijian.groupwallet.userservice.v1.RemoveGroupRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;

import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class RemoveGroupRequestProcessor {
    @Bean
    public Function<KStream<String, RemoveGroupRequest>, Function<GlobalKTable<String, User>, KStream<String, Status>>> removeGroup() {
        return removeGroupRequest -> userAggregateStore -> {

            var joined = removeGroupRequest.leftJoin(userAggregateStore, (leftKey, leftValue) -> leftValue.getUser(), RequestAndExistingUser::new);

            var userExistsValidationResult = validateUserExists(joined);

            var groupExistsValidationResult = validateGroupExists(userExistsValidationResult.getPassedStream());

            groupExistsValidationResult.getPassedStream()
                    .map(((key, value) -> KeyValue.pair(value.getRequest().getUser(), value.getRequest().getGroup())))
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                    .to("groupwallet.userservice.GroupRemoved-events");

            var user = groupExistsValidationResult
                    .getPassedStream()
                    .mapValues(value -> value.getCurrentUser().toBuilder()
                            .clearGroups()
                            .addAllGroups(value.getCurrentUser().getGroupsList().stream().filter(group -> !group.equals(value.getRequest().getGroup())).collect(Collectors.toList()))
                            .setAggregateVersion(value.getCurrentUser().getAggregateVersion() + 1)
                            .setEtag(UserAggregateUtil.calculateEtag(value.getCurrentUser().getName(), value.getCurrentUser().getAggregateVersion() + 1))
                            .build());

            var successStatus = user.mapValues(value -> OkStatusUtil.packStatus(value, "Group removed."));

            return userExistsValidationResult.getStatusStream()
                    .merge(groupExistsValidationResult.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, RequestAndExistingUser<RemoveGroupRequest>> validateUserExists(KStream<String, RequestAndExistingUser<RemoveGroupRequest>> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.currentUserExists()));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("User with name %s does not exists.", value.getRequest().getUser()))
                        .build());

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, RequestAndExistingUser<RemoveGroupRequest>> validateGroupExists(KStream<String, RequestAndExistingUser<RemoveGroupRequest>> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getCurrentUser().getGroupsList().contains(value.getRequest().getGroup())));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("Group with name %s does not exists.", value.getRequest().getGroup()))
                        .build());

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }
}
