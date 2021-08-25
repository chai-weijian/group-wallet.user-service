/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaiweijian.groupwallet.userservice.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

/**
 * Factory for creating serializers / deserializers.
 */
public class CustomSerdes {

    static public final class ListSerde<Inner> extends Serdes.WrapperSerde<List<Inner>> {

        final static int NULL_ENTRY_VALUE = -1;

        enum SerializationStrategy {
            CONSTANT_SIZE,
            VARIABLE_SIZE;

            public static final SerializationStrategy[] VALUES = SerializationStrategy.values();
        }

        public ListSerde() {
            super(new ListSerializer<>(), new ListDeserializer<>());
        }

        public <L extends List<Inner>> ListSerde(Class<L> listClass, Serde<Inner> serde) {
            super(new ListSerializer<>(serde.serializer()), new ListDeserializer<>(listClass, serde.deserializer()));
        }

    }

    /**
     * Construct a serde object from separate serializer and deserializer
     *
     * @param serializer    must not be null.
     * @param deserializer  must not be null.
     */
    static public <T> Serde<T> serdeFrom(final Serializer<T> serializer, final Deserializer<T> deserializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        if (deserializer == null) {
            throw new IllegalArgumentException("deserializer must not be null");
        }

        return new Serdes.WrapperSerde<>(serializer, deserializer);
    }

    /*
     * A serde for {@code List} type
     */
    static public <L extends List<Inner>, Inner> Serde<List<Inner>> ListSerde(Class<L> listClass, Serde<Inner> innerSerde) {
        return new ListSerde<>(listClass, innerSerde);
    }

}
