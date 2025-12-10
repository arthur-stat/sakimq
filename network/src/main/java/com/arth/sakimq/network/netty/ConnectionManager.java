package com.arth.sakimq.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 连接管理器，负责管理所有的连接对象
 */
public class ConnectionManager {
    private static final ConnectionManager INSTANCE = new ConnectionManager();
    private final ConcurrentMap<Channel, Connection> channelToConnection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> clientIdToConnection = new ConcurrentHashMap<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private ConnectionManager() {
    }

    /**
     * 获取连接管理器实例
     * @return ConnectionManager实例
     */
    public static ConnectionManager getInstance() {
        return INSTANCE;
    }

    /**
     * 创建连接对象
     * @param ctx ChannelHandlerContext
     * @param clientId 客户端ID
     * @return Connection对象
     */
    public Connection createConnection(ChannelHandlerContext ctx, String clientId) {
        Channel channel = ctx.channel();
        Connection connection = new Connection(channel, clientId);
        
        // 存储连接映射
        channelToConnection.put(channel, connection);
        clientIdToConnection.put(clientId, connection);
        connectionCount.incrementAndGet();
        
        return connection;
    }

    /**
     * 根据Channel获取连接对象
     * @param channel Channel对象
     * @return Connection对象
     */
    public Connection getConnection(Channel channel) {
        return channelToConnection.get(channel);
    }

    /**
     * 根据客户端ID获取连接对象
     * @param clientId 客户端ID
     * @return Connection对象
     */
    public Connection getConnection(String clientId) {
        return clientIdToConnection.get(clientId);
    }

    /**
     * 关闭连接
     * @param channel Channel对象
     */
    public void closeConnection(Channel channel) {
        Connection connection = channelToConnection.remove(channel);
        if (connection != null) {
            clientIdToConnection.remove(connection.getClientId());
            connection.close();
            connectionCount.decrementAndGet();
        }
    }

    /**
     * 关闭连接
     * @param clientId 客户端ID
     */
    public void closeConnection(String clientId) {
        Connection connection = clientIdToConnection.remove(clientId);
        if (connection != null) {
            channelToConnection.remove(connection.getChannel());
            connection.close();
            connectionCount.decrementAndGet();
        }
    }

    /**
     * 获取活跃连接数量
     * @return 活跃连接数量
     */
    public int getConnectionCount() {
        return connectionCount.get();
    }

    /**
     * 获取所有连接
     * @return 所有连接的Map
     */
    public Map<Channel, Connection> getAllConnections() {
        return channelToConnection;
    }
}