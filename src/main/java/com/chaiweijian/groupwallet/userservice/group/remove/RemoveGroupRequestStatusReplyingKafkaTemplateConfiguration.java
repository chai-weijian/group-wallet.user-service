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

package com.chaiweijian.groupwallet.userservice.group.remove;

import com.chaiweijian.groupwallet.userservice.v1.RemoveGroupRequest;
import com.google.rpc.Status;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class RemoveGroupRequestStatusReplyingKafkaTemplateConfiguration {
    private final ConsumerFactory<String, Status> consumerFactory;

    public RemoveGroupRequestStatusReplyingKafkaTemplateConfiguration(ConsumerFactory<String, Status> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Bean
    public ReplyingKafkaTemplate<String, RemoveGroupRequest, Status> removeGroupRequestStatusReplyingKafkaTemplate(
            ProducerFactory<String, RemoveGroupRequest> producerFactory,
            ConcurrentKafkaListenerContainerFactory<String, Status> containerFactory
    ) {
        containerFactory.setConsumerFactory(consumerFactory);
        ConcurrentMessageListenerContainer<String, Status> repliesContainer =
                containerFactory.createContainer("groupwallet.userservice.RemoveGroup-responses");
        repliesContainer.getContainerProperties().setGroupId("groupwallet.userservice.RemoveGroupRequest-group");
        repliesContainer.setAutoStartup(false);
        return new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
    }
}
