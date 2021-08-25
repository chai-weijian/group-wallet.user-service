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

import com.chaiweijian.groupwallet.userservice.util.PaginationUtil;
import com.chaiweijian.groupwallet.userservice.v1.AcceptGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.CreateGroupInvitationRequest;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitationAggregateServiceGrpc;
import com.chaiweijian.groupwallet.userservice.v1.ListGroupInvitationsRequest;
import com.chaiweijian.groupwallet.userservice.v1.ListGroupInvitationsResponse;
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
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@GrpcService
@Slf4j
public class GroupInvitationAggregateServer extends GroupInvitationAggregateServiceGrpc.GroupInvitationAggregateServiceImplBase {

    private final ReplyingKafkaTemplate<String, CreateGroupInvitationRequest, Status> createGroupInvitationTemplate;
    private final ReplyingKafkaTemplate<String, AcceptGroupInvitationRequest, Status> acceptGroupInvitationTemplate;
    private final ReplyingKafkaTemplate<String, RejectGroupInvitationRequest, Status> rejectGroupInvitationTemplate;
    private final InteractiveQueryService interactiveQueryService;

    public GroupInvitationAggregateServer(ReplyingKafkaTemplate<String, CreateGroupInvitationRequest, Status> createGroupInvitationTemplate,
                                          ReplyingKafkaTemplate<String, AcceptGroupInvitationRequest, Status> acceptGroupInvitationTemplate,
                                          ReplyingKafkaTemplate<String, RejectGroupInvitationRequest, Status> rejectGroupInvitationTemplate,
                                          InteractiveQueryService interactiveQueryService) {
        this.createGroupInvitationTemplate = createGroupInvitationTemplate;
        this.acceptGroupInvitationTemplate = acceptGroupInvitationTemplate;
        this.rejectGroupInvitationTemplate = rejectGroupInvitationTemplate;
        this.interactiveQueryService = interactiveQueryService;
    }

    @Override
    public void listGroupInvitations(ListGroupInvitationsRequest request, StreamObserver<ListGroupInvitationsResponse> responseObserver) {
        final ReadOnlyKeyValueStore<String, ArrayList<String>> groupInvitationIndexStore
                = interactiveQueryService.getQueryableStore("groupwallet.userservice.GroupInvitation-index", QueryableStoreTypes.keyValueStore());

        var groupInvitationIndex = groupInvitationIndexStore.get(request.getParent());

        if (groupInvitationIndex == null) {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(Code.NOT_FOUND_VALUE)
                            .setMessage(String.format("No group invitations for user %s.", request.getParent()))
                            .build()));
            return;
        }

        final ReadOnlyKeyValueStore<String, GroupInvitation> groupInvitationStore
                = interactiveQueryService.getQueryableStore("groupwallet.userservice.GroupInvitationAggregate-store", QueryableStoreTypes.keyValueStore());

        int index = 0;
        if (!request.getPageToken().isEmpty()) {
            index = PaginationUtil.decodePageToken(request.getPageToken());
        }

        var builder = ListGroupInvitationsResponse.newBuilder()
                .addAllGroupInvitations(groupInvitationIndex.subList(index, Math.min(index + request.getPageSize(), groupInvitationIndex.size()))
                        .stream().map(groupInvitationStore::get).collect(Collectors.toList()));

        if (index + request.getPageSize() < groupInvitationIndex.size()) {
            builder.setNextPageToken(PaginationUtil.encodePageToken(index + request.getPageSize()));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
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
