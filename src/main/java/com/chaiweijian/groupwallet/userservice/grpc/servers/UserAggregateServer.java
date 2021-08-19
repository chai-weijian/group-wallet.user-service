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

package com.chaiweijian.groupwallet.userservice.grpc.servers;

import com.chaiweijian.groupwallet.userservice.v1.*;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;

import java.util.concurrent.TimeUnit;

@GrpcService
@Slf4j
public class UserAggregateServer extends UserAggregateServiceGrpc.UserAggregateServiceImplBase {

    private final ReplyingKafkaTemplate<String, CreateUserRequest, Status> template;
    private final InteractiveQueryService interactiveQueryService;

    public UserAggregateServer(ReplyingKafkaTemplate<String, CreateUserRequest, Status> template, InteractiveQueryService interactiveQueryService) {
        this.template = template;
        this.interactiveQueryService = interactiveQueryService;
    }

    @Override
    public void createUser(CreateUserRequest request, StreamObserver<User> responseObserver) {
        ProducerRecord<String, CreateUserRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.CreateUser-requests",
                request.getUser().getUid(),
                request);

        RequestReplyFuture<String, CreateUserRequest, Status> replyFuture = template.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);

            if (consumerRecord.value().getCode() == 0) {
                // if the response is not error, the first detail will be the User created.
                Any detail = consumerRecord.value().getDetails(0);
                responseObserver.onNext(detail.unpack(User.class));
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(StatusProto.toStatusRuntimeException(consumerRecord.value()));
            }
        } catch (Exception exception) {
            log.error("UserAggregateServer - createUser Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void getUser(GetUserRequest request, StreamObserver<User> responseObserver) {
        var user = getUser(request.getName());
        if (user != null) {
            responseObserver.onNext(user);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(5)
                            .setMessage(String.format("%s does not exists.", request.getName()))
                            .build()));
        }
    }

    @Override
    public void findUser(FindUserRequest request, StreamObserver<User> responseObserver) {
        super.findUser(request, responseObserver);
    }

    // get user from queryable store
    // if the name does not match any key, return null
    private User getUser(String name) {
        // if the name is invalid, pretty sure the user
        // with invalid name is not exists in store
        if (!isValidUserNameFormat(name)) {
            return null;
        }

        final ReadOnlyKeyValueStore<String, User> userAggregateStore
                = interactiveQueryService.getQueryableStore("groupwallet.userservice.UserAggregate-store", QueryableStoreTypes.keyValueStore());
        return userAggregateStore.get(name);
    }

    // simple validation on name
    private Boolean isValidUserNameFormat(String name) {
        final String prefix = "users/";
        var hasValidPrefix = name.startsWith(prefix);
        // 4-63 characters user id with prefix users/
        var isValidLength = name.length() >= prefix.length() + 4 && name.length() <= prefix.length() + 63;

        return hasValidPrefix && isValidLength;
    }
}
