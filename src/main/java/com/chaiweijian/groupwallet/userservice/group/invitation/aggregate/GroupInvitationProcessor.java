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

package com.chaiweijian.groupwallet.userservice.group.invitation.aggregate;

import com.chaiweijian.groupwallet.userservice.util.UserAggregateUtil;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class GroupInvitationProcessor {
    private final KafkaProtobufSerde<GroupInvitation> groupInvitationSerde;

    public GroupInvitationProcessor(KafkaProtobufSerde<GroupInvitation> groupInvitationSerde) {
        this.groupInvitationSerde = groupInvitationSerde;
    }

    @Bean
    public Function<KStream<String, GroupInvitation>, KStream<String, GroupInvitation>> aggregateGroupInvitation() {
        return (groupInvitationCreated) -> {

            var userCreatedEvent = groupInvitationCreated
                    .selectKey(((key, value) -> value.getName()))
                    .groupByKey();

            return userCreatedEvent
                    .cogroup(EventHandler::handleGroupInvitationCreatedEvent)
                    .aggregate(() -> null,
                            Materialized.<String, GroupInvitation, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.GroupInvitation-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(groupInvitationSerde))
                    .toStream();
        };
    }

    private static class EventHandler {
        public static GroupInvitation handleGroupInvitationCreatedEvent(String key, GroupInvitation groupInvitation, GroupInvitation init) {
            var aggregateVersion = 1;
            return groupInvitation.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(UserAggregateUtil.calculateEtag(aggregateVersion))
                    .build();
        }
    }
}
