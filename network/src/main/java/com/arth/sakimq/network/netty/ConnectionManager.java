package com.arth.sakimq.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection manager that tracks all active connections.
 */
public class ConnectionManager {
    private static final ConnectionManager INSTANCE = new ConnectionManager();
    private final ConcurrentMap<Channel, Connection> channelToConnection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> clientIdToConnection = new ConcurrentHashMap<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private ConnectionManager() {
    }

    /**
     * @return singleton instance
     */
    public static ConnectionManager getInstance() {
        return INSTANCE;
    }

    /**
     * Create and register a connection.
     * @param ctx channel context
     * @param clientId client identifier
     * @return connection
     */
    public Connection createConnection(ChannelHandlerContext ctx, String clientId) {
        Channel channel = ctx.channel();
        Connection connection = new Connection(channel, clientId);
        
        // Store connection mappings
        channelToConnection.put(channel, connection);
        clientIdToConnection.put(clientId, connection);
        connectionCount.incrementAndGet();
        
        return connection;
    }

    /**
     * Get connection by channel.
     * @param channel netty channel
     * @return connection or null
     */
    public Connection getConnection(Channel channel) {
        return channelToConnection.get(channel);
    }

    /**
     * Get connection by client id.
     * @param clientId client identifier
     * @return connection or null
     */
    public Connection getConnection(String clientId) {
        return clientIdToConnection.get(clientId);
    }

    /**
     * Close connection by channel.
     * @param channel netty channel
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
     * Close connection by client id.
     * @param clientId client identifier
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
     * @return active connection count
     */
    public int getConnectionCount() {
        return connectionCount.get();
    }

    /**
     * @return map of channel to connection
     */
    public Map<Channel, Connection> getAllConnections() {
        return channelToConnection;
    }
}
