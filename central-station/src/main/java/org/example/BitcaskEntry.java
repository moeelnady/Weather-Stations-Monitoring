package org.example;

public class BitcaskEntry {
    private int key;
    private byte[] data;
    private int dataSize;
    private long timestamp;
    public BitcaskEntry(int key, byte[] data,long timestamp) {
        this.key = key;
        this.data = data;
        this.dataSize = data.length;
        this.timestamp = timestamp;
    }

    public int getKey() { return key; }
    public byte[] getData() { return data; }
    public int getDataSize() { return dataSize; }
    public long getTimestamp() { return timestamp; }

}
