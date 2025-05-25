package org.example;

import java.io.IOException;

import org.example.bitcask.Bitcask;
import org.example.bitcask.net.BitcaskServer;

public class BitcaskMain {

    public static void main(String[] args) throws IOException {
        Bitcask bitcask = new Bitcask("./bitcask-data");
        bitcask.scheduleCompaction();
        try (BitcaskServer server = new BitcaskServer(bitcask, 1022)) {
            server.start();
        }
    }
}
