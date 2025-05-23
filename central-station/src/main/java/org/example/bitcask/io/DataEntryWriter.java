package org.example.bitcask.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.example.bitcask.entry.DataEntry;
import org.example.bitcask.entry.ValueLocation;

public class DataEntryWriter implements Closeable {
    private static final long MAX_FILE_SIZE = 10 * 1024;
    private final FileResolver resolver;
    private long activeFileId;
    private RandomAccessFile activeFile;

    public DataEntryWriter(FileResolver resolver) {
        this.resolver = resolver;
    }

    public void openNewDataFile() throws IOException {
        if (activeFile != null) activeFile.close();
        activeFileId = System.currentTimeMillis();
        File file = resolver.getDataFile(activeFileId);
        activeFile = new RandomAccessFile(file, "rw");
    }

    public ValueLocation write(DataEntry entry) throws IOException {
        if (activeFile == null || activeFile.length() >= MAX_FILE_SIZE)
            openNewDataFile();
        long offset = activeFile.length();
        entry.write(activeFile);
        return new ValueLocation(activeFileId, offset, entry);
    }

    public long getActiveFileId() {
        return activeFileId;
    }

    public static long getMaxFileSize() {
        return MAX_FILE_SIZE;
    }

    public FileResolver getResolver() {
        return resolver;
    }

    public RandomAccessFile getActiveFile() {
        return activeFile;
    }

    @Override
    public void close() throws IOException {
        if (activeFile == null) return;
        activeFile.close();
    }

}
