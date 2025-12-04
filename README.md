# SakiMQ

SakiMQ is a lightweight message queue system implemented in Java.

> This is a wheel project. Its purpose is not to create a high-performance, highly-available message queue, but to implement a practically usable MQ within the scope of my current skills.

> Why is this project named SakiMQ? 'cause I stan [Tenma Saki](https://pjsekai.sega.jp/character/unite01/saki/index.html)!

# Implemented Features

Here are some scattered features in the process of design and implementation.

- Publish-Subscribe Model & Observer Pattern & Push Mode.
- The explicit queue leverages Disruptor with its zero-copy capability, a design approach more akin to that of RocketMQ.
- Push model supported (based on Disruptor 3.4.4) (similar to RabbitMQ).
- Support various exchange strategies (inspired by RabbitMQ).
- The integration of Netty's I/O multiplexing and JDK21's virtual threads enables high-performance concurrency with a concise synchronous coding style.
- Components communicate efficiently via Protobuf.

# TO DO

- Pull mode optional.