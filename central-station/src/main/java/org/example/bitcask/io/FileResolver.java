package org.example.bitcask.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileResolver {
    private final Path directory;
    private Map<Long, Integer> readersCounter;
    private Set<Long> toBeDeleted;

    public FileResolver(Path directory) {
        this.directory = directory;
        this.readersCounter = new HashMap<>();
        this.toBeDeleted = new HashSet<>();
    }
    public static long toFileId(Path path) {
        String[] splitted = path.getFileName().toString().split("\\.");
        return Long.parseLong(splitted[0]);
    }

    public static boolean isHintFile(Path path) {
        return path.getFileName().toString().matches("[1-9]\\d*.bitcask.hint");
    }

    public static boolean isDataFile(Path path) {
        return path.getFileName().toString().matches("[1-9]\\d*.bitcask.data");
    }

    public File getHintFile(long fileId) {
        return new File(directory.toFile(), String.format("%d.bitcask.hint", fileId));
    }

    public File getDataFile(long fileId) {
        return new File(directory.toFile(), String.format("%d.bitcask.data", fileId));
    }

    public Path getDirectory() {
        return directory;
    }

    private void delete(long fileId) throws IOException {
        Files.deleteIfExists(getDataFile(fileId).toPath());
        Files.deleteIfExists(getHintFile(fileId).toPath());
    }

    public synchronized File startRead(long fileId) {
        File file = getDataFile(fileId);
        int count = readersCounter.getOrDefault(fileId, 0);
        readersCounter.put(fileId, count + 1);
        return file;
    }

    public synchronized void finishRead(long fileId) throws IOException {
        int count = readersCounter.get(fileId);
        if (count != 1) {
            readersCounter.put(fileId, count - 1);
            return;
        }
        readersCounter.remove(fileId);
        if (toBeDeleted.remove(fileId)) {
            delete(fileId);
        }
    }

    public synchronized void markToBeDeleted(long fileId) throws IOException {
        if (!readersCounter.containsKey(fileId)) {
            delete(fileId);
            return;
        }
        toBeDeleted.add(fileId);
    }

    @Override
    public String toString() {
        return "FileResolver [readersCounter=" + readersCounter + ", toBeDeleted=" + toBeDeleted + "]";
    }

}
