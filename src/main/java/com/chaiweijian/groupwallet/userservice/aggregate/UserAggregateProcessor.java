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

import com.chaiweijian.groupwallet.userservice.util.UserAggregateUtil;
import com.chaiweijian.groupwallet.userservice.v1.User;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

@Component
public class UserAggregateProcessor {

    private final KafkaProtobufSerde<User> userSerde;

    public UserAggregateProcessor(KafkaProtobufSerde<User> userSerde) {
        this.userSerde = userSerde;
    }

    // Maintain user aggregate by subscribing to all events that
    // affect a user.
    @Bean
    public BiFunction<KStream<String, User>, KStream<String, User>, KStream<String, User>> aggregateUser() {
        return (userCreated, userUpdated) -> {

            var userCreatedEvent = userCreated.groupByKey();
            var userUpdatedEvent = userUpdated.groupByKey();

            return userCreatedEvent
                    .cogroup(EventHandler::handleUserCreatedEvent)
                    .cogroup(userUpdatedEvent, EventHandler::handleUserUpdatedEvent)
                    .aggregate(() -> null,
                        Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("groupwallet.userservice.UserAggregate-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userSerde))
                    .toStream();
        };
    }

    private static class EventHandler {

        // Simply update aggregate version and return the new user
        public static User handleUserCreatedEvent(String key, User user, User init) {
            var aggregateVersion = 1;
            return user.toBuilder().setAggregateVersion(aggregateVersion).setEtag(UserAggregateUtil.calculateEtag(user.getName(), aggregateVersion)).build();
        }

        // UserUpdated event provide a full user object, simply increment aggregate version and return it
        public static User handleUserUpdatedEvent(String key, User user, User init) {
            var aggregateVersion = init.getAggregateVersion() + 1;
            return user.toBuilder().setAggregateVersion(aggregateVersion).setEtag(UserAggregateUtil.calculateEtag(user.getName(), aggregateVersion)).build();
        }
    }
}
