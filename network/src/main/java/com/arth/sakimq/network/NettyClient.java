package com.arth.sakimq.network;

// import com.google.protobuf.ByteString;
import com.arth.sakimq.protocol.MessageProto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.concurrent.CompletableFuture;

/**
 * For client
 */
public class NettyClient {

    private final String host;
    private final int port;
    private Channel clientChannel;
    private EventLoopGroup workerGroup;
    private CompletableFuture<Void> connectionFuture = new CompletableFuture<>();

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        /* Recommended way in Netty 4.2: https://netty.io/wiki/netty-4.2-migration-guide.html */
        workerGroup = new MultiThreadIoEventLoopGroup(
                0,  // default thread num: 2 * CPU cores
                NioIoHandler.newFactory()
        );

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            /* ProtoBuf decoder & encoder */
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                            pipeline.addLast("decoder", new ProtobufDecoder(MessageProto.getDefaultInstance()));
                            pipeline.addLast(new LengthFieldPrepender(4));
                            pipeline.addLast("encoder", new ProtobufEncoder());

                            // 添加业务处理器
                            pipeline.addLast("clientHandler", new SimpleChannelInboundHandler<MessageProto>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    System.out.println("Connected to server: " + ctx.channel().remoteAddress());
                                    clientChannel = ctx.channel();
                                    connectionFuture.complete(null);
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                    System.out.println("Disconnected from server");
                                    connectionFuture.completeExceptionally(new RuntimeException("Connection lost"));
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, MessageProto msg) throws Exception {
                                    System.out.println("Received from server: " + msg);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            clientChannel = future.channel();

            // 等待通道关闭
            clientChannel.closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public void sendMessage(String message) {
//        if (clientChannel != null && clientChannel.isActive()) {
//            // 创建一个示例消息
//            MessageProto msg = MessageProto.newBuilder()
//                    .setMessageId(System.currentTimeMillis())
//                    .setBody(ByteString.copyFromUtf8(message))
//                    .setTimestamp(System.currentTimeMillis())
//                    .build();
//            clientChannel.writeAndFlush(msg);
//        }
    }

    public CompletableFuture<Void> getConnectionFuture() {
        return connectionFuture;
    }

    public void disconnect() {
        if (clientChannel != null) {
            clientChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}