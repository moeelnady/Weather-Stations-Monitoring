package org.example.bitcask.entry;

import java.util.Comparator;
import java.util.function.Function;

public class ValueLocation implements Comparable<ValueLocation>{
    private int valueSize;
    private long valuePosition;
    private long timestamp;
    private long fileId;

    public ValueLocation(long fileId, long offset, DataEntry dataEntry) {
        this.valueSize = dataEntry.getValueSize();
        this.valuePosition = offset + dataEntry.getEntrySize() - dataEntry.getValueSize();
        this.timestamp = dataEntry.getTimestamp();
        this.fileId = fileId;
    }

    public ValueLocation(long fileId, HintEntry hintEntry) {
        this.valueSize = hintEntry.getValueSize();
        this.valuePosition = hintEntry.getValuePosition();
        this.timestamp = hintEntry.getTimestamp();
        this.fileId = fileId;
    }

    public <T> T to(Function<? super ValueLocation, T> mapper) {
        return mapper.apply(this);
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

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    @Override
    public int compareTo(ValueLocation o) {
        return Comparator.comparingLong(ValueLocation::getTimestamp)
                         .thenComparingLong(ValueLocation::getFileId)
                         .thenComparingLong(ValueLocation::getValuePosition)
                         .compare(this, o);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + valueSize;
        result = prime * result + (int) (valuePosition ^ (valuePosition >>> 32));
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        result = prime * result + (int) (fileId ^ (fileId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ValueLocation other = (ValueLocation) obj;
        if (valueSize != other.valueSize) return false;
        if (valuePosition != other.valuePosition) return false;
        if (timestamp != other.timestamp) return false;
        if (fileId != other.fileId) return false;
        return true;
    }

}
