package org.example;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class BitcaskEngine {
    private static final String DATA_DIR = "bitcask-data";
    private static final long MAX_FILE_SIZE = 10 * 1024; // 10KB

    private RandomAccessFile currentDataFile;
    private final FileHandler fileHandler;
    private final AtomicLong currentFileId;
    private final Map<Integer, FileOffset> index = new HashMap<>();

    public BitcaskEngine() throws IOException {
        Files.createDirectories(Paths.get(DATA_DIR));
        this.fileHandler = new FileHandler();
        this.currentFileId = new AtomicLong(0);

        recoverFromHintFiles(); // try to load existing index
        openNewDataFile();      // open new file after recovery
    }

    private void openNewDataFile() throws IOException {
        currentFileId.incrementAndGet();
        currentDataFile = new RandomAccessFile(getFilePath(currentFileId.get()), "rw");
        currentDataFile.seek(currentDataFile.length());
    }

    public synchronized void put(int key, byte[] value) throws IOException {
        long timestamp = System.currentTimeMillis();
        BitcaskEntry entry = new BitcaskEntry(key, value, timestamp);
        long offset = currentDataFile.length();
        fileHandler.writeEntry(currentDataFile, entry);
        index.put(key, new FileOffset(currentFileId.get(), offset, timestamp));

        if (currentDataFile.length() >= MAX_FILE_SIZE) {
            currentDataFile.getFD().sync();
            currentDataFile.close();

            // write hint file
            String hintPath = getHintFilePath(currentFileId.get());
            fileHandler.writeHintFile(hintPath, index);
            openNewDataFile();
        }
    }
    public synchronized String get(int key) throws IOException {
        FileOffset offset = index.get(key);
        if (offset == null) return null;

        try (RandomAccessFile file = new RandomAccessFile(getFilePath(offset.getFileId()), "r")) {
            file.seek(offset.getOffsetVal());
            BitcaskEntry entry = fileHandler.readEntry(file);
            return entry == null ? null : new String(entry.getData());
        }
    }

    private String getFilePath(long fileId) {
        return DATA_DIR + "/segment_" + fileId + ".data";
    }
    private String getHintFilePath(long fileId) {
        return DATA_DIR + "/segment_" + fileId + ".hint";
    }
    private void recoverFromHintFiles() throws IOException {
        File dir = new File(DATA_DIR);
        File[] hintFiles = dir.listFiles((d, name) -> name.endsWith(".hint"));

        if (hintFiles != null) {
            for (File hint : hintFiles) {
                Map<Integer, FileOffset> recovered = fileHandler.readHintFile(hint.getPath());
                for (Map.Entry<Integer, FileOffset> entry : recovered.entrySet()) {
                    int key = entry.getKey();
                    FileOffset existing = index.get(key);
                    if (existing == null || entry.getValue().getTimestamp() > existing.getTimestamp()) {
                        index.put(key, entry.getValue());
                    }
                }
                long fid = extractFileId(hint.getName());
                if (fid > currentFileId.get()) currentFileId.set(fid);
            }
        }
    }

    private long extractFileId(String fileName) {
        // fileName = segment_#.hint
        return Long.parseLong(fileName.replace("segment_", "").replace(".hint", ""));
    }

    public Set<Integer> getAllKeys() {
        return new HashSet<>(index.keySet());
    }
}
