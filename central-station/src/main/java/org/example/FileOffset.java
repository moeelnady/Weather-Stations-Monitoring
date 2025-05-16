package org.example;

import java.util.concurrent.atomic.AtomicLong;

public class FileOffset {
    private final long fileId;
    private final long offsetVal;

    public FileOffset(long fileId, long offsetVal) {
        this.fileId = fileId;
        this.offsetVal = offsetVal;
    }

    public long getFileId() {
        return fileId;
    }

    public long getOffsetVal() {
        return offsetVal;
    }

}
