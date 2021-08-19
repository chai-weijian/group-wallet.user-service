package com.chaiweijian.groupwallet.userservice;

import com.chaiweijian.groupwallet.userservice.v1.CreateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.chaiweijian.groupwallet.userservice.v1.UserCreated;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public KafkaProtobufSerde<UserCreated> userCreatedKafkaProtobufSerde() {
        final KafkaProtobufSerde<UserCreated> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(UserCreated.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<User> userKafkaProtobufSerde() {
        final KafkaProtobufSerde<User> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(User.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @Bean
    public KafkaProtobufSerde<Status> statusKafkaProtobufSerde() {
        final KafkaProtobufSerde<Status> protobufSerde = new KafkaProtobufSerde<>();
        protobufSerde.configure(getSerdeConfig(Status.class.getCanonicalName()), false);
        return protobufSerde;
    }

    @NotNull
    private Map<String, String> getSerdeConfig(String specificProtobufValueType) {
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        serdeConfig.put(SPECIFIC_PROTOBUF_VALUE_TYPE, specificProtobufValueType);
        return serdeConfig;
    }
}
