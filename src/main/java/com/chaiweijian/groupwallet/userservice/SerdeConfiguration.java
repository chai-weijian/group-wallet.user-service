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

package com.chaiweijian.groupwallet.userservice;

import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.userservice.v1.AcceptGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.CreateGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.CreateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import com.chaiweijian.groupwallet.userservice.v1.RejectGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.UpdateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.chaiweijian.groupwallet.userservice.v1.RemoveGroupRequest;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE;

@Configuration
public class SerdeConfiguration {
    @Bean
    public KafkaProtobufSerde<CreateUserRequest> createUserRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<CreateUserRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(CreateUserRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<UpdateUserRequest> updateUserRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<UpdateUserRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(UpdateUserRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<User> userKafkaProtobufSerde() {
        final KafkaProtobufSerde<User> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(User.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<Group> groupKafkaProtobufSerde() {
        final KafkaProtobufSerde<Group> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(Group.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<GroupInvitation> groupInvitationKafkaProtobufSerde() {
        final KafkaProtobufSerde<GroupInvitation> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(GroupInvitation.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<RemoveGroupRequest> removeGroupRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<RemoveGroupRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(RemoveGroupRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<CreateGroupInvitationRequest> createGroupInvitationRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<CreateGroupInvitationRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(CreateGroupInvitationRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<AcceptGroupInvitationRequest> acceptGroupInvitationRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<AcceptGroupInvitationRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(AcceptGroupInvitationRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<RejectGroupInvitationRequest> rejectGroupInvitationRequestKafkaProtobufSerde() {
        final KafkaProtobufSerde<RejectGroupInvitationRequest> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(RejectGroupInvitationRequest.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<Status> statusKafkaProtobufSerde() {
        final KafkaProtobufSerde<Status> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(Status.class.getCanonicalName()), false);
        return protobufSerde;
    }

    private Map<String, String> getSerdeConfig(String specificProtobufValueType) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, specificProtobufValueType);
        return serdeConfig;
    }
}
