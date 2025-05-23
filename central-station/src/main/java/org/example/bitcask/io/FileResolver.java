package org.example.bitcask.io;

import java.io.File;
import java.nio.file.Path;

public class FileResolver {
    private final Path directory;

    public FileResolver(Path directory) {
        this.directory = directory;
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
}
