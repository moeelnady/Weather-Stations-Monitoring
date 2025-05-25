package org.example.bitcask.net;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.example.bitcask.net.exception.OperationFailedException;
import org.example.bitcask.net.exception.UnknownStatusException;
import org.example.bitcask.net.protocol.Operation;
import org.example.bitcask.net.protocol.Result;

public class BitcaskClient implements Closeable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public BitcaskClient(String host, int port) throws UnknownHostException, IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    public void put(String key, byte[] value) throws IOException, OperationFailedException, UnknownStatusException {
        byte[] keyBytes = key.getBytes();
        int keySize = keyBytes.length;
        int valueSize = value.length;
        out.write(Operation.PUT);
        out.writeInt(keySize);
        out.writeInt(valueSize);
        out.write(keyBytes);
        out.write(value);
        int status = in.read();
        switch (status) {
            case Result.SUCCESS:
                return;
            case Result.FAILURE:
                throw new OperationFailedException("put operation failed");
            default:
                throw new UnknownStatusException("server sent a unknown status", status);
        }
    }

    public byte[] get(String key) throws IOException, OperationFailedException, UnknownStatusException {
        byte[] keyBytes = key.getBytes();
        int keySize = keyBytes.length;
        out.write(Operation.GET);
        out.writeInt(keySize);
        out.write(keyBytes);
        int status = in.read();
        switch (status) {
            case Result.SUCCESS:
                int valueSize = in.readInt();
                byte[] value = new byte[valueSize];
                in.readFully(value);
                return value;
            case Result.NULL:
                return null;
            case Result.FAILURE:
                throw new OperationFailedException("get operation failed");
            default:
                throw new UnknownStatusException("server sent a unknown status", status);
        }
    }

    public Map<String, byte[]> getAll() throws IOException, OperationFailedException, UnknownStatusException {
        Map<String, byte[]> result = new HashMap<>();
        out.write(Operation.GET_ALL);
        int status = in.read();
        switch (status) {
            case Result.SUCCESS:
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    int keySize = in.readInt();
                    int valueSize = in.readInt();
                    byte[] keyBytes = new byte[keySize];
                    byte[] value = new byte[valueSize];
                    in.readFully(keyBytes);
                    in.readFully(value);
                    String key = new String(keyBytes);
                    result.put(key, value);
                }
                return result;
            case Result.FAILURE:
                throw new OperationFailedException("get operation failed");
            default:
                throw new UnknownStatusException("server sent a unknown status", status);
        }
    }

    public byte[] getOrDefault(String key, byte[] defaultValue) throws IOException, OperationFailedException, UnknownStatusException {
        byte[] result = get(key);
        return result == null ? defaultValue : result;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
