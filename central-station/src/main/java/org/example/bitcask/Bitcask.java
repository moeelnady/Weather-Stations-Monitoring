package org.example.bitcask;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.example.bitcask.entry.DataEntry;
import org.example.bitcask.entry.HintEntry;
import org.example.bitcask.entry.ValueLocation;
import org.example.bitcask.io.DataEntryWriter;
import org.example.bitcask.io.DataEntryWriterPlus;
import org.example.bitcask.io.FileResolver;

public class Bitcask {
    private final FileResolver resolver;
    private final DataEntryWriter writer;
    private final ConcurrentMap<String, ValueLocation> keydir;
    private final static long MINIMUM_NUMBER_OF_FILES = 10;
    private final static long INITIAL_COMPACTION_DELAY = 60;
    private final static long COMPACTION_PERIOD = 60;

    public Bitcask(String directory) throws IOException {
        this(Path.of(directory));
    }

    public Bitcask(Path directory) throws IOException {
        try {
            Files.createDirectories(directory);
        } catch (FileAlreadyExistsException e) {
            System.out.println("path is not a directory.");
            System.exit(-1);
        }
        this.resolver = new FileResolver(directory);
        this.writer = new DataEntryWriter(resolver);
        this.keydir = new ConcurrentHashMap<>();
        constructKeydir();
        this.writer.openNewDataFile();
    }

    public void keydirUpdate(String key, ValueLocation location) {
        ValueLocation oldLocation = keydir.get(key);
        if (oldLocation == null || oldLocation.compareTo(location) < 0) keydir.put(key, location);
    }

    public void processHintFile(long fileId) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(resolver.getHintFile(fileId));
        DataInputStream inputStream = new DataInputStream(fileInputStream);
        HintEntry entry = new HintEntry();
        try (inputStream) {
            while (true) {
                boolean wasRead = entry.read(inputStream);
                if (!wasRead) break;
                keydirUpdate(entry.getKey(), new ValueLocation(fileId, entry));
            }
        }
    }

    public void processDataFile(long fileId) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(resolver.getDataFile(fileId));
        DataInputStream inputStream = new DataInputStream(fileInputStream);
        DataEntry entry = new DataEntry();
        long offset = 0;
        try (inputStream) {
            while (true) {
                boolean wasRead = entry.read(inputStream);
                if (!wasRead) break;
                keydirUpdate(entry.getKey(), new ValueLocation(fileId, offset, entry));
                offset += entry.getEntrySize();
            }
        }
    }

    public void compactDataFile(long dataFileId, DataEntryWriterPlus compactFileWriter) throws IOException {
        File dataFile = resolver.startRead(dataFileId);
        if (dataFile == null) return;
        FileInputStream fileInputStream = new FileInputStream(dataFile);
        DataInputStream inputStream = new DataInputStream(fileInputStream);
        DataEntry entry = new DataEntry();
        long offset = 0;
        try (inputStream) {
            while (true) {
                boolean wasRead = entry.read(inputStream);
                if (!wasRead) break;
                ValueLocation dataFileLocation = new ValueLocation(dataFileId, offset, entry);
                offset += entry.getEntrySize();
                if (!keydir.get(entry.getKey()).equals(dataFileLocation)) continue;
                ValueLocation compactFileLocation = compactFileWriter.write(entry);
                keydir.replace(entry.getKey(), dataFileLocation, compactFileLocation);
            }
        }
        resolver.finishRead(dataFileId);
        resolver.markToBeDeleted(dataFileId);
    }

    public void constructKeydir() throws IOException {
        List<Long> fileIds = Files.list(resolver.getDirectory())
                                  .filter(FileResolver::isDataFile)
                                  .map(FileResolver::toFileId)
                                  .collect(Collectors.toList());
        for (Long fileId : fileIds) {
            if (resolver.getHintFile(fileId).exists())
                processHintFile(fileId);
            else
                processDataFile(fileId);
        }
    }

    public void compact() throws IOException {
        List<Long> oldFilesIds = Files.list(resolver.getDirectory())
                                      .filter(FileResolver::isDataFile)
                                      .map(FileResolver::toFileId)
                                      .filter((id) -> id != writer.getActiveFileId())
                                      .collect(Collectors.toList());
        if (oldFilesIds.size() < MINIMUM_NUMBER_OF_FILES) return;
        DataEntryWriterPlus compactFileWriter = new DataEntryWriterPlus(new DataEntryWriter(resolver));
        try (compactFileWriter) {
            for (Long dataFileId : oldFilesIds) {
                compactDataFile(dataFileId, compactFileWriter);
            }
        }
    }

    public ScheduledExecutorService scheduleCompaction() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable runnable = () -> {
            try {
                compact();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        scheduler.scheduleWithFixedDelay(runnable, INITIAL_COMPACTION_DELAY, COMPACTION_PERIOD, TimeUnit.SECONDS);
        return scheduler;
    }

    public synchronized void put(String key, byte[] value) throws IOException {
        DataEntry entry = new DataEntry(key, value);
        keydir.put(key, writer.write(entry));
    }

    public byte[] get(String key) throws IOException {
        ValueLocation location = null;
        File file = null;
        synchronized (resolver) {
            location = keydir.get(key);
            if (location == null) return null;
            file = resolver.startRead(location.getFileId());
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(location.getValuePosition());
        byte[] bytes = new byte[location.getValueSize()];
        randomAccessFile.readFully(bytes);
        randomAccessFile.close();
        resolver.finishRead(location.getFileId());
        return bytes;
    }

    public byte[] getOrDefault(String key, byte[] defaultValue) throws IOException {
        byte[] result = get(key);
        return result == null ? defaultValue : result;
    }
}
