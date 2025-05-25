package org.example.bitcask.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

public class DataEntry {
    private long timestamp;
    private String key;
    private byte[] value;

    public DataEntry() {}

    public DataEntry(String key, byte[] value) {
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    public boolean read(DataInput input) throws IOException {
        try {
            timestamp = input.readLong();
            int keySize = input.readInt();
            int valueSize = input.readInt();
            byte[] keyBytes = new byte[keySize];
            value = new byte[valueSize];
            input.readFully(keyBytes);
            input.readFully(value);
            key = new String(keyBytes);
            return true;
        } catch (EOFException e) {
           return false;
        }
    }

    public void write(DataOutput output) throws IOException {
        byte[] keyBytes = key.getBytes();
        int keySize = keyBytes.length;
        output.writeLong(timestamp);
        output.writeInt(keySize);
        output.writeInt(value.length);
        output.write(keyBytes);
        output.write(value);
    }

    public long getEntrySize() {
        return Long.BYTES + Integer.BYTES * 2 + key.getBytes().length + value.length;
    }

    public long getValuePositionInEntry() {
        return Long.BYTES + Integer.BYTES * 2 + key.getBytes().length;
    }

    public int getValueSize() {
        return value.length;
    }

    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }
    public byte[] getValue() {
        return value;
    }
    public void setValue(byte[] value) {
        this.value = value;
    }

}
