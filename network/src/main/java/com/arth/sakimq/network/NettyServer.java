package com.arth.sakimq.network;

// import com.google.protobuf.ByteString;
import com.arth.sakimq.protocol.MessageProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * For broker
 */
public class NettyServer {

    private final int port;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        /* Recommended way in Netty 4.2: https://netty.io/wiki/netty-4.2-migration-guide.html */
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(
                0,  // default thread num: 2 * CPU cores
                NioIoHandler.newFactory()
        );

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();

                            /* ProtoBuf decoder & encoder */
                            pipeline.addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                            pipeline.addLast("decoder", new ProtobufDecoder(MessageProto.getDefaultInstance()));
                            pipeline.addLast(new LengthFieldPrepender(4));
                            pipeline.addLast("encoder", new ProtobufEncoder());

                            // 添加业务处理器
                            pipeline.addLast("serverHandler", new SimpleChannelInboundHandler<MessageProto>() {
                                @Override
                                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                    System.out.println("Client connected: " + ctx.channel().remoteAddress());
                                }

                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, MessageProto msg) throws Exception {
                                    System.out.println("Received from client: " + msg);

                                    // Echo the message back to client
                                    ctx.writeAndFlush(msg);
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                    cause.printStackTrace();
                                    ctx.close();
                                }
                            });
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            System.out.println("Server started on port " + port);

            // 等待通道关闭
            serverChannel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}