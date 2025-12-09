package com.arth.sakimq.test;

import com.arth.sakimq.broker.config.QueueConfig;
import com.arth.sakimq.clients.config.DefaultProducerConfig;
import com.arth.sakimq.clients.config.ProducerConfig;
import com.arth.sakimq.network.config.DefaultNettyConfig;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.common.utils.YamlParser;

import java.util.Map;

public class ConfigTest {
    public static void main(String[] args) {
        System.out.println("Testing configuration loading...");
        
        // Test YamlParser directly
        testYamlParser();
        
        // Test NettyConfig
        testNettyConfig();
        
        // Test ProducerConfig
        testProducerConfig();
        
        // Test QueueConfig
        testQueueConfig();
        
        System.out.println("All configuration tests passed!");
    }
    
    private static void testYamlParser() {
        System.out.println("\n1. Testing YamlParser...");
        try {
            Map<String, String> configMap = YamlParser.parseYaml("config/network.yml");
            System.out.println("   Loaded network.yml: " + configMap);
            
            configMap = YamlParser.parseYaml("config/producer.yml");
            System.out.println("   Loaded producer.yml: " + configMap);
            
            configMap = YamlParser.parseYaml("config/queue.yml");
            System.out.println("   Loaded queue.yml: " + configMap);
        } catch (Exception e) {
            System.err.println("   Error loading config files: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testNettyConfig() {
        System.out.println("\n2. Testing NettyConfig...");
        try {
            NettyConfig config = DefaultNettyConfig.getConfig();
            System.out.println("   Port: " + config.getPort());
            System.out.println("   Timeout: " + config.getTimeout());
            System.out.println("   MaxFrameLength: " + config.getMaxFrameLength());
            System.out.println("   LengthFieldLength: " + config.getLengthFieldLength());
            System.out.println("   InitialBytesToStrip: " + config.getInitialBytesToStrip());
        } catch (Exception e) {
            System.err.println("   Error loading NettyConfig: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testProducerConfig() {
        System.out.println("\n3. Testing ProducerConfig...");
        try {
            ProducerConfig config = DefaultProducerConfig.getConfig();
            System.out.println("   MaxRetries: " + config.getMaxRetries());
        } catch (Exception e) {
            System.err.println("   Error loading ProducerConfig: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testQueueConfig() {
        System.out.println("\n4. Testing QueueConfig...");
        try {
            QueueConfig config = QueueConfig.getConfig();
            System.out.println("   Timeout: " + config.getTimeout());
            System.out.println("   BufferSize: " + config.getBufferSize());
            System.out.println("   ConsumerGroupSize: " + config.getConsumerGroupSize());
        } catch (Exception e) {
            System.err.println("   Error loading QueueConfig: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
