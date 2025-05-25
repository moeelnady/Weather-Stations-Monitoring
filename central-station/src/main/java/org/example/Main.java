package org.example;

import org.example.bitcask.Bitcask;
import org.example.bitcask.net.BitcaskServer;

public class Main {
    public static void main(String[] args) throws Exception {
        Bitcask bitcask = new Bitcask("./bitcask-data/");
        bitcask.scheduleCompaction();
        try (BitcaskServer server = new BitcaskServer(bitcask, 1022)) {
            server.start();
        }
    }
}
