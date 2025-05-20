package org.example;

import java.util.concurrent.atomic.AtomicLong;

public class FileOffset {
    private final long fileId;
    private final long offset;
    private final long timestamp;

    public FileOffset(long fileId, long offset, long timestamp) {
        this.fileId = fileId;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public long getFileId() {
        return fileId;
    }

    public long getOffsetVal() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
