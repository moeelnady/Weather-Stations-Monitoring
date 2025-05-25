package org.example.shell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.example.bitcask.net.BitcaskClient;

public class BitcaskShell {
    private static String SHELL;
    private static String SHELL_CMD_FLAG;

    public static String colored(String msg, int... ansiCodes) {
        if (ansiCodes == null || ansiCodes.length == 0) return msg;
        StringBuilder builder = new StringBuilder();
        builder.append("\u001b[");
        for (int i = 0; i < ansiCodes.length; i++) {
            if (i != 0) builder.append(";");
            builder.append(ansiCodes[i]);
        }
        builder.append("m");
        builder.append(msg);
        builder.append("\u001b[0m");
        return builder.toString();
    }

    public static boolean get(String[] args) {
        if (args.length != 2 || !args[0].equals("view")) return false;
        try (BitcaskClient client = new BitcaskClient("localhost", 1022)) {
            byte[] result = client.get(args[1]);
            if (result == null) {
                System.out.printf("received %s\n", colored("null", 31));
            } else {
                System.out.println(new String(result));
            }
        } catch (Exception e) {
            System.out.println(colored("FAILURE", 31));
        }
        return true;
    }

    public static boolean set(String[] args) {
        if (args.length < 3 || !args[0].equals("set")) return false;
        try (BitcaskClient client = new BitcaskClient("localhost", 1022)) {
            String value = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
            client.put(args[1], value.getBytes());
            System.out.println(colored("SUCCESS", 32));
        } catch (Exception e) {
            System.out.println(colored("FAILURE", 31));
        }
        return true;
    }

    public static void getAllHandler(BitcaskClient client, String suffix) throws Exception {
        Map<String, byte[]> result = client.getAll();
        String filename = "%d%s".formatted(System.currentTimeMillis(), suffix);
        PrintWriter out = new PrintWriter(filename);
        try (out) {
            for (var entry : result.entrySet()) {
                out.printf("%s,%s\n", entry.getKey(), new String(entry.getValue()));
            }
        }
    }

    public static boolean getAll(String[] args) {
        if (args.length != 1 || !args[0].equals("viewall")) return false;
        try (BitcaskClient client = new BitcaskClient("localhost", 1022)) {
            Map<String, byte[]> result = client.getAll();
            for (var entry : result.entrySet()) {
                System.out.printf("%s,%s\n", entry.getKey(), new String(entry.getValue()));
            }
            System.out.println(colored("SUCCESS", 32));
        } catch (Exception e) {
            System.out.println(colored("FAILURE", 31));
        }
        return true;
    }

    public static boolean performance(String[] args) {
        if (args.length != 2 || !args[0].equals("perf")) return false;
        try {
            int number = Integer.parseInt(args[1]);
            Thread[] threads = new Thread[number];
            for (int i = 0; i < number; i++) {
                final int j = i;
                Runnable runnable = () -> {
                    try (BitcaskClient threadClient = new BitcaskClient("localhost", 1022)) {
                        getAllHandler(threadClient, "_thread_%d.csv".formatted(j));
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("thread %d failed".formatted(j));
                    }
                };
                threads[i] = new Thread(runnable);
                threads[i].start();
            }
            for (int i = 0; i < number; i++) {
                threads[i].join();
            }
            System.out.println(colored("FINISHED", 32));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(colored("FAILURE", 31));
        }
        return true;
    }

    public static boolean runInShell(String[] args) {
        if (args.length == 0 || !args[0].startsWith("!")) return false;
        args[0] = args[0].substring(1);
        List<String> command = new ArrayList<>();
        command.add(SHELL);
        command.add(SHELL_CMD_FLAG);
        for (String arg : args)
            command.add(arg);
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.inheritIO();
        Process process;
        try {
            process = builder.start();
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(colored("FAILURE", 31));
        }
        return true;
    }

    private static void process(String[] args) {
        if (get(args) || getAll(args) || set(args) || performance(args) || runInShell(args)) {
            return;
        }
        if(args[0].equals("exit")) System.exit(0);
        System.out.println(colored("ERROR", 31));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            SHELL = "cmd.exe";
            SHELL_CMD_FLAG = "/c";
        } else {
            SHELL = "bash";
            SHELL_CMD_FLAG = "-c";
        }
        if (args.length != 0) {
            process(args);
            return;
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.print(">>> ");
        String line = null;
        while ((line = in.readLine()) != null) {
            process(line.split(" "));
            System.out.print(">>> ");
        }
    }

}
