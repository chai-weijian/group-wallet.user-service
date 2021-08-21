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

package com.chaiweijian.groupwallet.userservice.aggregate;

import com.chaiweijian.groupwallet.userservice.v1.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class UidNameMapProcessor {
    // In the system, a user is identified by the name.
    // When user login with Firebase, the only information the app know
    // is the uid, so maintain a one-to-one map from uid to name
    @Bean
    public Consumer<KStream<String, User>> mapUidToName() {
        return userCreated -> userCreated
                .map((key, value) -> KeyValue.pair(value.getUid(), value.getName()))
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.UidNameMap-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));
    }
}
