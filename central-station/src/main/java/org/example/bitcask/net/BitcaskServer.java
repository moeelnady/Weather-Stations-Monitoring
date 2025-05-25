package org.example.bitcask.net;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.example.bitcask.Bitcask;

public class BitcaskServer implements Closeable {
    private final ServerSocket serverSocket;
    private final ExecutorService threadpool;
    private final Bitcask bitcask;

    public BitcaskServer(Bitcask bitcask, int port) throws IOException {
        this.bitcask = bitcask;
        this.serverSocket = new ServerSocket(port);
        this.threadpool = Executors.newFixedThreadPool(100);
    }

    public void start() {
        while (true) {
            try{
                Socket client = serverSocket.accept();
                threadpool.execute(new BitcaskClientHandler(bitcask, client));
            } catch(IOException e) {
                e.printStackTrace();
                System.out.print("couldn't connect with the client");
            }
        }
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
    }
}
