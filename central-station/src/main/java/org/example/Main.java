package org.example;

import java.io.IOException;
import java.nio.file.Path;
import org.example.bitcask.Bitcask;

public class Main {
    public static void main(String[] args) throws Exception {
        Bitcask x = new Bitcask(Path.of("../bitcask-data/"));
        // x.put("1", "this is one".getBytes());
        // x.put("2", "this is two".getBytes());
        // x.put("3", "this is three".getBytes());
        Thread reader1 = new Thread(() -> {
            while (true) {
                try {
                    System.out.println(new String(x.getOrDefault("1", new byte[]{1})));
                    Thread.sleep(1000);
                } catch (IOException e) {
                    try {
                        System.in.read();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread reader2 = new Thread(() -> {
            while (true) {
                try {
                    System.out.println(new String(x.getOrDefault("2", new byte[]{2})));
                    Thread.sleep(1000);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        Thread reader3 = new Thread(() -> {
            while (true) {
                try {
                    System.out.println(new String(x.getOrDefault("3", new byte[]{1})));
                    Thread.sleep(1000);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        Thread compacter = new Thread(() -> {
            while (true) {
                try {
                    x.compact();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        reader1.start();
        reader2.start();
        reader3.start();
        compacter.start();
    }
}
