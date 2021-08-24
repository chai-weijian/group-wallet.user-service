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

import com.chaiweijian.groupwallet.userservice.v1.AcceptGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.CreateGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitationAggregateServiceGrpc;
import com.chaiweijian.groupwallet.userservice.v1.RejectGroupInvitationRequest;
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
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;

import java.util.concurrent.TimeUnit;

@GrpcService
@Slf4j
public class GroupInvitationAggregateServer extends GroupInvitationAggregateServiceGrpc.GroupInvitationAggregateServiceImplBase {

    private final ReplyingKafkaTemplate<String, CreateGroupInvitationRequest, Status> createGroupInvitationTemplate;
    private final ReplyingKafkaTemplate<String, AcceptGroupInvitationRequest, Status> acceptGroupInvitationTemplate;
    private final ReplyingKafkaTemplate<String, RejectGroupInvitationRequest, Status> rejectGroupInvitationTemplate;

    public GroupInvitationAggregateServer(ReplyingKafkaTemplate<String, CreateGroupInvitationRequest, Status> createGroupInvitationTemplate,
                                          ReplyingKafkaTemplate<String, AcceptGroupInvitationRequest, Status> acceptGroupInvitationTemplate,
                                          ReplyingKafkaTemplate<String, RejectGroupInvitationRequest, Status> rejectGroupInvitationTemplate) {
        this.createGroupInvitationTemplate = createGroupInvitationTemplate;
        this.acceptGroupInvitationTemplate = acceptGroupInvitationTemplate;
        this.rejectGroupInvitationTemplate = rejectGroupInvitationTemplate;
    }

    @Override
    public void createGroupInvitation(CreateGroupInvitationRequest request, StreamObserver<GroupInvitation> responseObserver) {
        ProducerRecord<String, CreateGroupInvitationRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.CreateGroupInvitation-requests",
                request.getParent(),
                request);

        RequestReplyFuture<String, CreateGroupInvitationRequest, Status> replyFuture = createGroupInvitationTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("GroupInvitationAggregateServer - createGroupInvitation Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void rejectGroupInvitation(RejectGroupInvitationRequest request, StreamObserver<GroupInvitation> responseObserver) {
        ProducerRecord<String, RejectGroupInvitationRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.RejectGroupInvitation-requests",
                request.getGroupInvitation().getName(),
                request);

        RequestReplyFuture<String, RejectGroupInvitationRequest, Status> replyFuture = rejectGroupInvitationTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("GroupInvitationAggregateServer - rejectGroupInvitation Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void acceptGroupInvitation(AcceptGroupInvitationRequest request, StreamObserver<GroupInvitation> responseObserver) {
        ProducerRecord<String, AcceptGroupInvitationRequest> record = new ProducerRecord<>(
                "groupwallet.userservice.AcceptGroupInvitation-requests",
                request.getGroupInvitation().getName(),
                request);

        RequestReplyFuture<String, AcceptGroupInvitationRequest, Status> replyFuture = acceptGroupInvitationTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("GroupInvitationAggregateServer - acceptGroupInvitation Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    private void handleResponse(ConsumerRecord<String, Status> consumerRecord,
                                StreamObserver<GroupInvitation> responseObserver) throws InvalidProtocolBufferException {
        if (consumerRecord.value().getCode() == Code.OK_VALUE) {
            // if the response is not error, the first detail will be the user created/updated.
            Any detail = consumerRecord.value().getDetails(0);
            responseObserver.onNext(detail.unpack(GroupInvitation.class));
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(consumerRecord.value()));
        }
    }
}