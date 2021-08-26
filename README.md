# Group Wallet

This website is part my _Group Wallet_ project.

You can find other part of this project in following links:
- [protobuf](https://github.com/chai-weijian/group-wallet.protobuf)
- [website](https://github.com/chai-weijian/group-wallet.website)
- [group-service](https://github.com/chai-weijian/group-wallet.group-service)
- [rest-api](https://github.com/chai-weijian/group-wallet.rest-api)

## Architecture

### gRPC layer
The gRPC servers responsible to fill the gap between REST API request/response model and Event-driven design asynchronous model. [Sprint for Apache Kafka's](https://spring.io/projects/spring-kafka) ReplyingKafkaTemplate helps to achieve this.  

### Event Sourcing
All requests from client, for example CreateUserRequest, UpdateUserRequest, etc..., will be handled by a stream processor. The processor do the validation, return a response, and also dispatch domain event.

The aggregate processor will subscribe to events and build an aggregate. The aggregate is then materialized to state store, readily available to be queried with [interactive queries](https://kafka.apache.org/10/documentation/streams/developer-guide/interactive-queries.html). The store is exposed to outside world with gRPC.

## Project Status - work in progress

I will progressively implement my ideas to this project. As I learn more and more technologies, the list of ideas are also expanding.

This project will be mark as completed only when I stop learning any new technologies, which probably wouldn't happen.

## Technologies
This service is build with some of my favourite back-end technologies:
- [Kafka](https://kafka.apache.org/)
- [Spring boot](https://spring.io/)
- [gRPC](https://grpc.io/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)