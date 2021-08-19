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
import com.chaiweijian.groupwallet.userservice.v1.UserCreated;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
public class UserAggregateProcessor {

    private final KafkaProtobufSerde<User> userSerde;

    public UserAggregateProcessor(KafkaProtobufSerde<User> userSerde) {
        this.userSerde = userSerde;
    }

    // When a user is created, the system need to have a way
    // to retrieve the user by uid or name
    // This processor prepares the system to serve both queries
    @Bean
    public Consumer<KStream<String, UserCreated>> initializeUser() {
        return userCreated -> {
            // The common way of retrieving a single user is by using the resource name
            // Simply initialize the user in store when user is created, subsequent update
            // of the user should be handled by separate processor
            userCreated.peek(((key, value) -> System.out.printf("%s, %s", key, value))).mapValues(UserCreated::getUser)
                    .groupByKey()
                    .aggregate(() -> null, (aggKey, value, agg) -> value.toBuilder().setAggregateVersion(1).build(),
                            Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.UserAggregate-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(userSerde));

            // When a user sign in with Firebase, the only information we get is the uid,
            // so here materialize an Uid to user-name map to allow retrieving user by uid
            userCreated.mapValues(UserCreated::getUser)
                    .groupBy((key, value) -> value.getUid(), Grouped.with(Serdes.String(), userSerde))
                    .aggregate(String::new, (aggKey, value, agg) -> value.getName(),
                            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.UidNameMap-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(Serdes.String()));
        };
    }
}
