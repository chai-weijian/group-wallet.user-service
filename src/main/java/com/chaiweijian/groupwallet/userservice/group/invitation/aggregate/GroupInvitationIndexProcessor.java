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

import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import com.chaiweijian.groupwallet.userservice.serialization.CustomSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

@Component
public class GroupInvitationIndexProcessor {

    private final KafkaProtobufSerde<GroupInvitation> groupInvitationSerde;
    private final Serde<List<String>> listSerde = CustomSerdes.ListSerde(ArrayList.class, Serdes.String());

    public GroupInvitationIndexProcessor(KafkaProtobufSerde<GroupInvitation> groupInvitationSerde) {
        this.groupInvitationSerde = groupInvitationSerde;
    }

    /**
     * build an index to help list all group invitations under a user
     */
    @Bean
    public Consumer<KStream<String, GroupInvitation>> buildGroupInvitationIndex() {
        return groupInvitationCreated -> groupInvitationCreated
                .selectKey(((key, value) -> getParentName(value.getName())))
                .repartition(Repartitioned.with(Serdes.String(), groupInvitationSerde))
                .groupByKey()
                .aggregate(ArrayList::new, (aggKey, newValue, aggValue) -> buildIndex((ArrayList<String>) aggValue, newValue.getName()),
                        Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.GroupInvitation-index")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(listSerde));
    }

    private static String getParentName(String name) {
        var pattern = Pattern.compile("(users/[a-z0-9-]+)/groupInvitations/[a-z0-9-]+");
        var matcher = pattern.matcher(name);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new RuntimeException(String.format("Group Invitation with invalid name: %s", name));
        }
    }

    private ArrayList<String> buildIndex(ArrayList<String> aggValue, String newValue) {
        aggValue.add(0, newValue);
        return aggValue;
    }
}
