package org.example.bitcask.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import org.example.bitcask.Bitcask;
import org.example.bitcask.net.protocol.Operation;
import org.example.bitcask.net.protocol.Result;

public class BitcaskClientHandler implements Runnable {
    private final Socket socket;
    private final Bitcask bitcask;

    public BitcaskClientHandler(Bitcask bitcask, Socket socket) {
        this.socket = socket;
        this.bitcask = bitcask;
    }

    private void handleGet(DataInputStream in, DataOutputStream out) throws IOException {
        int keySize = in.readInt();
        byte[] keyBytes = new byte[keySize];
        in.readFully(keyBytes);
        String key = new String(keyBytes);
        byte[] result = null;
        try {
            result = bitcask.get(key);
        } catch (IOException e) {
            out.write(Result.FAILURE);
        }
        if (result == null) {
            out.write(Result.NULL);
        } else {
            out.write(Result.SUCCESS);
            out.writeInt(result.length);
            out.write(result);
        }
    }

    private void handlePut(DataInputStream in, DataOutputStream out) throws IOException {
        int keySize = in.readInt();
        int valueSize = in.readInt();
        byte[] keyBytes = new byte[keySize];
        byte[] value = new byte[valueSize];
        in.readFully(keyBytes);
        in.readFully(value);
        String key = new String(keyBytes);
        try {
            bitcask.put(key, value);
            out.write(Result.SUCCESS);
        } catch (IOException e) {
            out.write(Result.FAILURE);
        }
    }

    public void handleGetAll(DataInputStream in, DataOutputStream out) throws IOException {
        Map<String, byte[]> result = null;
        try {
            result = bitcask.getAll();
        } catch (IOException e) {
            out.write(Result.FAILURE);
        }
        out.write(Result.SUCCESS);
        out.writeInt(result.size());
        for (var entry : result.entrySet()) {
            byte[] value = entry.getValue();
            byte[] keyBytes = entry.getKey().getBytes();
            int keySize = keyBytes.length;
            int valueSize = value.length;
            out.writeInt(keySize);
            out.writeInt(valueSize);
            out.write(keyBytes);
            out.write(value);
        }
    }

    public void run() {
        try (socket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            while (true) {
                int operation = in.read();
                if (operation == -1) break;
                switch (operation) {
                    case Operation.GET:
                        handleGet(in, out);
                        break;
                    case Operation.PUT:
                        handlePut(in, out);
                        break;
                    case Operation.GET_ALL:
                        handleGetAll(in, out);
                        break;
                    default:
                        out.write(Result.WTF);
                        break;
                }
            }
            System.out.println("client closed connection in a normal way");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("client closed connection but not in a normal way");
        }
    }

}
