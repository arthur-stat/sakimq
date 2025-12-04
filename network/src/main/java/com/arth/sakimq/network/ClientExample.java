package com.arth.sakimq.network;

public class ClientExample {
    public static void main(String[] args) {
        NettyClient client = new NettyClient("localhost", 8080);
        try {
            client.connect();
        } catch (InterruptedException e) {
            System.err.println("Client was interrupted");
            e.printStackTrace();
        }
    }
}