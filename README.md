# Under construction...

# SakiMQ

SakiMQ is a lightweight message queue system implemented in Java.

> This is a wheel project. Its purpose is not to create a high-performance, highly-available message queue, but to implement a practically usable MQ within the scope of my current skills.

> Why is this project named SakiMQ? 'cause I stan [Tenma Saki](https://pjsekai.sega.jp/character/unite01/saki/index.html)!

# Implemented Features

- Publish-Subscribe Model & Observer Pattern & Pull Mode.
- The explicit queue leverages Disruptor with its zero-copy capability, a design approach more akin to that of RocketMQ.
- Support various exchange strategies (inspired by RabbitMQ).
- The integration of Netty's I/O multiplexing and JDK21's virtual threads enables high-performance concurrency with a concise synchronous coding style.
- Components communicate efficiently via Protobuf.
- Multiple design patterns were applied. For example, use the builder pattern for the creation of Exchange.
- At-least-once delivery is guaranteed through the ACK confirmation mechanism, while at-most-once delivery is ensured through idempotent processing between the Producer and the Broker.
- Adopts RFC 1982 serial number arithmetic, ensuring correct and overflow-safe sequence comparison on a wrap-around 64-bit counter.

# TO DO

- Push mode may be optional.