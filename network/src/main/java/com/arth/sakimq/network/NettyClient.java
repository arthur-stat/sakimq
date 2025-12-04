package com.arth.sakimq.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

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

                            // 添加编解码器
                            pipeline.addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));

                            // 添加业务处理器
                            pipeline.addLast("clientHandler", new SimpleChannelInboundHandler<String>() {
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
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
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
        if (clientChannel != null && clientChannel.isActive()) {
            clientChannel.writeAndFlush(message);
        }
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