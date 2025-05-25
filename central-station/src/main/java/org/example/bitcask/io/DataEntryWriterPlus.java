package org.example.bitcask.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.example.bitcask.entry.DataEntry;
import org.example.bitcask.entry.HintEntry;
import org.example.bitcask.entry.ValueLocation;

public class DataEntryWriterPlus implements Closeable {
    private final DataEntryWriter writer;
    private final FileResolver resolver;
    private long activeHintFileId;
    private RandomAccessFile activeHintFile;


    public DataEntryWriterPlus(DataEntryWriter writer) {
        this.writer = writer;
        this.resolver = writer.getResolver();
    }

    private void openHintFile(long fileId) throws IOException {
        if (activeHintFileId == fileId && activeHintFile != null) return;
        if (activeHintFile != null) activeHintFile.close();
        File file = resolver.getHintFile(fileId);
        activeHintFileId = fileId;
        activeHintFile = new RandomAccessFile(file, "rw");
    }

    public void openNewDataFile() throws IOException {
        writer.openNewDataFile();
    }

    public ValueLocation write(DataEntry entry) throws IOException {
        ValueLocation location = writer.write(entry);
        long fileId = location.getFileId();
        openHintFile(fileId);
        HintEntry hintEntry = new HintEntry();
        hintEntry.setValueSize(location.getValueSize());
        hintEntry.setValuePosition(location.getValuePosition());
        hintEntry.setTimestamp(location.getTimestamp());
        hintEntry.setKey(entry.getKey());
        hintEntry.write(activeHintFile);
        return location;
    }

    public long getActiveFileId() {
        return writer.getActiveFileId();
    }

    @Override
    public void close() throws IOException {
        writer.close();
        if (activeHintFile == null) return;
        activeHintFile.close();
    }
}
