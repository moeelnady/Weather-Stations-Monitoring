package org.example.bitcask.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

public class HintEntry {
    private int valueSize;
    private long valuePosition;
    private long timestamp;
    private String key;

    public boolean read(DataInput input) throws IOException {
        try {
            timestamp = input.readLong();
            int keySize = input.readInt();
            valueSize = input.readInt();
            valuePosition = input.readLong();
            byte[] keyBytes = new byte[keySize];
            input.readFully(keyBytes);
            key = new String(keyBytes);
            return true;
        } catch (EOFException e) {
            return false;
        }
    }

    public void write(DataOutput output) throws IOException {
        output.writeLong(timestamp);
        byte[] keyBytes = key.getBytes();
        output.writeInt(keyBytes.length);
        output.writeInt(valueSize);
        output.writeLong(valuePosition);
        output.write(keyBytes);
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    public long getValuePosition() {
        return valuePosition;
    }

    public void setValuePosition(long valuePosition) {
        this.valuePosition = valuePosition;
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
}
