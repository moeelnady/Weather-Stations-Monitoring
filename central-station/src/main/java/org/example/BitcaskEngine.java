package org.example;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BitcaskEngine {
    private static final String DATA_DIR = "bitcask-data";
    private static final long MAX_FILE_SIZE = 10 * 1024; // 10KB
    private RandomAccessFile currentDataFile ;
    private FileHandler fileHandler;
    private AtomicLong currentFileId;
    private final Map<Integer, FileOffset> index = new HashMap<>();
    public BitcaskEngine() throws IOException {
        Files.createDirectories(Paths.get(DATA_DIR));
        currentFileId = new AtomicLong(0);
        fileHandler = new FileHandler();
        openNewDataFile();
    }
    private void openNewDataFile() throws IOException {
        currentFileId.incrementAndGet();
        currentDataFile = new RandomAccessFile(getFilePath(currentFileId.get()), "rw");
        currentDataFile.seek(currentDataFile.length());
    }
    public synchronized void put (int key, byte[] value) throws IOException {
        BitcaskEntry entry = new BitcaskEntry(key, value,System.currentTimeMillis());
        FileOffset offset = new FileOffset(currentFileId.get(),currentDataFile.length());
        index.put(key,offset);
        fileHandler.writeEntry(currentDataFile,entry);
        if (currentDataFile.length() >= MAX_FILE_SIZE) {
            currentDataFile.close();
            openNewDataFile();
        }
    }
    public synchronized String get(int key) throws IOException {
        FileOffset offset = index.get(key);
        if (offset == null) return null;
        String filePath = getFilePath(offset.getFileId());

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(offset.getOffsetVal());
            BitcaskEntry entry = fileHandler.readEntry(file);
            return entry == null ? null : new String(entry.getData());
        }
    }
    private String getFilePath(long fileId) {
        return DATA_DIR + "/segment_" + fileId + ".data";
    }
    public Set<Integer> getAllKeys() {
            return new HashSet<>(index.keySet());
    }


}
