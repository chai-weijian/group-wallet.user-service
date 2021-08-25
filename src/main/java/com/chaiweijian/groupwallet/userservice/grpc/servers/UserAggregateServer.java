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

import com.chaiweijian.groupwallet.userservice.v1.CreateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.FindUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.GetUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.RemoveGroupRequest;
import com.chaiweijian.groupwallet.userservice.v1.UpdateUserRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.chaiweijian.groupwallet.userservice.v1.UserAggregateServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
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

    private final ReplyingKafkaTemplate<String, CreateUserRequest, Status> createUserTemplate;
    private final ReplyingKafkaTemplate<String, UpdateUserRequest, Status> updateUserTemplate;
    private final ReplyingKafkaTemplate<String, RemoveGroupRequest, Status> removeGroupTemplate;
    private final InteractiveQueryService interactiveQueryService;

    public UserAggregateServer(ReplyingKafkaTemplate<String, CreateUserRequest, Status> createUserTemplate,
                               ReplyingKafkaTemplate<String, UpdateUserRequest, Status> updateUserTemplate,
                               ReplyingKafkaTemplate<String, RemoveGroupRequest, Status> removeGroupTemplate,
                               InteractiveQueryService interactiveQueryService) {
        this.createUserTemplate = createUserTemplate;
        this.updateUserTemplate = updateUserTemplate;
        this.removeGroupTemplate = removeGroupTemplate;
        this.interactiveQueryService = interactiveQueryService;
    }

    @Override
    public void createUser(CreateUserRequest request, StreamObserver<User> responseObserver) {
        ProducerRecord<String, CreateUserRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.CreateUser-requests",
                request.getUser().getUid(),
                request);

        RequestReplyFuture<String, CreateUserRequest, Status> replyFuture = createUserTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("UserAggregateServer - createUser Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void getUser(GetUserRequest request, StreamObserver<User> responseObserver) {
        getUser(request.getName(), responseObserver);
    }

    @Override
    public void findUser(FindUserRequest request, StreamObserver<User> responseObserver) {
        final ReadOnlyKeyValueStore<String, String> userIdNameMapStore
                = interactiveQueryService.getQueryableStore("groupwallet.userservice.UidNameMap-store", QueryableStoreTypes.keyValueStore());

        var name = userIdNameMapStore.get(request.getUid());

        if (name != null) {
            getUser(name, responseObserver);
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(Code.NOT_FOUND_VALUE)
                            .setMessage(String.format("Uid %s does not exists.", request.getUid()))
                            .build()));
        }
    }

    // get user from queryable store
    private void getUser(String name, StreamObserver<User> responseObserver) {
        if (!isValidUserNameFormat(name)) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(Code.INVALID_ARGUMENT_VALUE)
                            .setMessage(String.format("%s is not a valid name format.", name))
                            .build()));
        }

        final ReadOnlyKeyValueStore<String, User> userAggregateStore
                = interactiveQueryService.getQueryableStore("groupwallet.userservice.UserAggregate-store", QueryableStoreTypes.keyValueStore());

        var user = userAggregateStore.get(name);

        if (user != null) {
            responseObserver.onNext(user);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(Code.NOT_FOUND_VALUE)
                            .setMessage(String.format("%s does not exists.", name))
                            .build()));
        }
    }

    @Override
    public void removeGroup(RemoveGroupRequest request, StreamObserver<User> responseObserver) {
        ProducerRecord<String, RemoveGroupRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.RemoveGroup-requests",
                request.getUser(),
                request);

        RequestReplyFuture<String, RemoveGroupRequest, Status> replyFuture = removeGroupTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("UserAggregateServer - removeGroup Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void updateUser(UpdateUserRequest request, StreamObserver<User> responseObserver) {
        ProducerRecord<String, UpdateUserRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.UpdateUser-requests",
                request.getUser().getName(),
                request);

        RequestReplyFuture<String, UpdateUserRequest, Status> replyFuture = updateUserTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("UserAggregateServer - updateUser Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    private void handleResponse(ConsumerRecord<String, Status> consumerRecord,
                                StreamObserver<User> responseObserver) throws InvalidProtocolBufferException {
        if (consumerRecord.value().getCode() == Code.OK_VALUE) {
            // if the response is not error, the first detail will be the user created/updated.
            Any detail = consumerRecord.value().getDetails(0);
            responseObserver.onNext(detail.unpack(User.class));
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(consumerRecord.value()));
        }
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
