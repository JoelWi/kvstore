package kvstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import kvstore.Result.Ok;
import kvstore.respDataType.bulkString;
import kvstore.respDataType.respArray;
import kvstore.respDataType.respInteger;
import kvstore.respDataType.simpleError;
import kvstore.respDataType.simpleString;

@FunctionalInterface
interface TriFunction<T, U, V, R> {
    R apply(T t, U u, V v);
}

sealed interface Result<T> {
    record Ok<T>(T value) implements Result<T> {
    };

    record Err<T>(Throwable err) implements Result<T> {
    };

    static <T> Result<T> of(Callable<T> code) {
        try {
            return new Ok<T>(code.call());
        } catch (Throwable throwable) {
            return new Err<T>(throwable);
        }
    }

    default <R> Result<R> map(Function<T, R> mapper) {
        return switch (this) {
            case Ok<T> ok -> Result.of(() -> mapper.apply(ok.value()));
            case Err<T> err -> new Err<R>(err.err());
        };
    }
};

record byteBuffer(byte[] buffer, int bytesRead) {
};

sealed interface respDataType {
    record simpleString(String value) implements respDataType {
    };

    record simpleError(String value) implements respDataType {
    };

    record respInteger(int value) implements respDataType {
    };

    record bulkString(String value) implements respDataType {
    };

    record respArray(String value) implements respDataType {
    }

}

public class App {
    static final char STRING = '+';
    static final char ERROR = '-';
    static final char INTEGER = ':';
    static final char BULK = '$';
    static final char ARRAY = '*';

    record value(String typ, String str, int num, String bulk, value[] array) {
    };

    Function<InputStream, Result<byteBuffer>> readBytes = is -> Result.of(() -> {
        var buffer = new byte[1024];
        var bytesRead = is.read(buffer);
        return new byteBuffer(buffer, bytesRead);
    });
    Function<Socket, Result<OutputStream>> outputStream = s -> Result.of(() -> s.getOutputStream());

    Function<Character, Boolean> isValidType = initialByte -> switch (initialByte) {
        case STRING -> true;
        case ERROR -> true;
        case INTEGER -> true;
        case BULK -> true;
        case ARRAY -> true;
        default -> false;
    };

    BiFunction<Character, String, respDataType> deserialiseData = (type, data) -> switch (type) {
        case STRING -> new simpleString(data);
        case ERROR -> new simpleError(data);
        case INTEGER -> new respInteger(Integer.valueOf(data));
        case BULK -> new bulkString(data);
        case ARRAY -> new respArray(data);
        default -> new simpleError("Invalid type: " + type);
    };

    Supplier<respDataType> ping = () -> new simpleString("+PONG\r\n");

    Map<String, String> store = new HashMap<>();
    Map<String, Map<String, String>> hashStore = new HashMap<>();

    BiFunction<String, String, String> setData = (key, value) -> {
        store.put(key, value);
        return "+OK\r\n";
    };

    Function<String, String> getData = key -> store.containsKey(key) ? "+" + store.get(key) + "\r\n" : "_\r\n";

    TriFunction<String, String, String, String> hsetData = (map, key, value) -> {
        if (!hashStore.containsKey(map)) {
            hashStore.put(map, new HashMap<>());
        }
        var selectedMap = hashStore.get(map);

        selectedMap.put(key, value);
        return "+OK\r\n";
    };

    BiFunction<String, String, String> hgetData = (map, key) -> {
        if (!hashStore.containsKey(map)) {
            return "_\r\n";
        }

        var selectedMap = hashStore.get(map);
        return selectedMap.containsKey(key) ? "+" + selectedMap.get(key) + "\r\n" : "_\r\n";
    };

    void readInput(byte[] buffer, Socket clientSocket, Path aof) {
        var initialByte = (char) buffer[0];
        System.out.println(initialByte);
        boolean validType = isValidType.apply(initialByte);
        var arrLen = Character.getNumericValue(buffer[1]);
        var items = new respDataType[arrLen];

        // 1. start at 4
        // 2. validate type
        // 3. increment once
        // 4. length of value
        // 5. increment thrice
        // 6. stringify the data
        // 7. deserialise to the correct type
        // 8. add to items
        // 9. increment by the value of length and 2
        //
        // 4 -> (type) -> +1 -> (len) -> +3 + (len + 2)
        IntStream.range(0, arrLen);
        // starting point of first item
        var ptr = 4;
        for (int i = 0; i < arrLen; ++i) {
            var type = (char) buffer[ptr];
            boolean valid = isValidType.apply(type);
            if (valid) {
                ptr++;
                var len = Character.getNumericValue(buffer[ptr]);
                // skipping over \r\n
                ptr += 3;
                var rawData = new String(buffer, ptr, len);
                var item = deserialiseData.apply(type, rawData);
                items[i] = item;
                // skipping over \r\n to next item
                ptr += len + 2;
            } else {
                System.out.println("broke on: " + ptr);
                System.out.println(type);
                break;
            }
        }

        var usedBytes = Arrays.copyOf(buffer, ptr);

        System.out.println("items:");
        Stream.of(items).forEach(System.out::println);

        var osRes = outputStream.apply(clientSocket);

        Result.of(() -> {
            var command = (bulkString) items[0];
            System.out.println("COMMAND: " + command.value());
            if (command.value().equals("PING")) {
                switch (items.length) {
                    case 1 -> {
                        var pong = ping.get();
                        if (osRes instanceof Ok<OutputStream> ok) {
                            var os = ok.value();
                            if (pong instanceof simpleString ss) {
                                os.write(ss.value().getBytes());
                            }
                        }
                    }
                    default -> {
                        var commandArgs = Stream
                                .of(items)
                                .skip(1)
                                .map(item -> switch (item) {
                                    case respInteger i -> String.valueOf(i.value());
                                    case simpleString ss -> ss.value();
                                    case simpleError se -> se.value();
                                    case bulkString bs -> bs.value();
                                    case respArray ra -> ra.value();
                                })
                                .reduce((s1, s2) -> s1 + " " + s2)
                                .get();
                        commandArgs = "+PONG \"" + commandArgs + "\"\r\n";

                        if (osRes instanceof Ok<OutputStream> ok) {
                            var os = ok.value();
                            os.write(commandArgs.getBytes());
                        }

                    }

                }
            } else if (command.value().equals("SET")) {
                var commandArgs = Stream
                        .of(items)
                        .skip(1)
                        .map(item -> switch (item) {
                            case respInteger i -> String.valueOf(i.value());
                            case simpleString ss -> ss.value();
                            case simpleError se -> se.value();
                            case bulkString bs -> bs.value();
                            case respArray ra -> ra.value();
                        })
                        .toList();
                var res = setData.apply(commandArgs.get(0), commandArgs.get(1));
                Result.of(() -> Files.write(aof, usedBytes, StandardOpenOption.APPEND));

                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();
                    os.write(res.getBytes());
                }

            } else if (command.value().equals("GET")) {
                var key = (bulkString) items[1];
                var res = getData.apply(key.value());

                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();
                    os.write(res.getBytes());
                }

            } else if (command.value().equals("HSET")) {
                var commandArgs = Stream
                        .of(items)
                        .skip(1)
                        .map(item -> switch (item) {
                            case respInteger i -> String.valueOf(i.value());
                            case simpleString ss -> ss.value();
                            case simpleError se -> se.value();
                            case bulkString bs -> bs.value();
                            case respArray ra -> ra.value();
                        })
                        .toList();
                var res = hsetData.apply(commandArgs.get(0), commandArgs.get(1),
                        commandArgs.get(2));
                Result.of(() -> Files.write(aof, usedBytes, StandardOpenOption.APPEND));

                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();
                    os.write(res.getBytes());
                }

            } else if (command.value().equals("HGET")) {
                var map = (bulkString) items[1];
                var key = (bulkString) items[2];
                var res = hgetData.apply(map.value(), key.value());
                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();
                    os.write(res.getBytes());
                }
            } else {
                System.out.println("invalid command");

                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();

                    switch (validType) {
                        case true -> os.write("+OK\r\n".getBytes());
                        case false -> os.write("-INVALID TYPE\r\n".getBytes());
                    }
                }
            }
            return true;
        });
    }

    void clientInput(InputStream is, Socket clientSocket, Path aof) {
        System.out.println("doing stuff");
        Stream.generate(() -> readBytes.apply(is))
                .takeWhile(r -> r instanceof Ok<byteBuffer> ok && ok.value().bytesRead() != -1)
                .map(r -> (Ok<byteBuffer>) r)
                .map(ok -> ok.value())
                .filter(bb -> bb.bytesRead() > 6)
                .forEach(bb -> {
                    var buffer = bb.buffer();
                    readInput(buffer, clientSocket, aof);
                });

    }

    public static void main(String[] args) throws UnknownHostException, IOException {

        var app = new App();

        var aof = Paths.get("aof");
        if (!Files.exists(aof)) {
            Files.createFile(aof);
        } else {
            var cmds = Files.readAllLines(aof);
            var logs = new ArrayList<ByteArrayInputStream>();
            var base = "";
            var len = 0;
            var iterations = 0;
            for (int i = 0; i < cmds.size(); i++) {
                var cmd = cmds.get(i);

                if (len == 0 && cmd.charAt(0) != '*') {
                    continue;
                }

                if (len == 0) {
                    System.out.println("here");
                    System.out.println(cmd);
                    len = Character.getNumericValue(cmd.charAt(1));
                    iterations = len * 2 + 1;
                }

                if (iterations != 0) {
                    base += cmd + "\r\n";
                    iterations--;
                }

                if (iterations == 0) {
                    app.readInput(base.getBytes(), new Socket(), Paths.get(""));
                    base = "";
                    len = 0;
                }

            }
        }

        try (var serverSocket = new ServerSocket(6379)) {
            System.out.println("Server started, waiting for connections...");
            while (true) {
                try (var clientSocket = serverSocket.accept()) {
                    System.out.println("Client connection accepted!");
                    var is = clientSocket.getInputStream();
                    app.clientInput(is, clientSocket, aof);
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
                System.out.println("Client disconnected.");
            }
        }
    }
}
