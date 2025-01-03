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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import kvstore.Result.Ok;
import kvstore.RespDataType.bulkString;
import kvstore.RespDataType.respArray;
import kvstore.RespDataType.respInteger;
import kvstore.RespDataType.simpleError;
import kvstore.RespDataType.simpleString;

record ByteBuffer(byte[] buffer, int bytesRead) {
};

public class App {
    static final char STRING = '+';
    static final char ERROR = '-';
    static final char INTEGER = ':';
    static final char BULK = '$';
    static final char ARRAY = '*';

    Function<InputStream, Result<ByteBuffer>> readBytes = is -> Result.of(() -> {
        var buffer = new byte[1024];
        var bytesRead = is.read(buffer);
        return new ByteBuffer(buffer, bytesRead);
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

    BiFunction<Character, String, RespDataType> deserialiseData = (type, data) -> switch (type) {
        case STRING -> new simpleString(data);
        case ERROR -> new simpleError(data);
        case INTEGER -> new respInteger(Integer.valueOf(data));
        case BULK -> new bulkString(data);
        case ARRAY -> new respArray(data);
        default -> new simpleError("Invalid type: " + type);
    };

    Supplier<simpleString> ping = () -> new simpleString("+PONG\r\n");

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

    record BufferResult(RespDataType[] items, int ptr) {
    };

    TailCall<BufferResult> readBuffer(int ptr, byte[] buffer, int arrLen, int index, RespDataType[] items) {
        var type = (char) buffer[ptr];
        boolean valid = isValidType.apply(type);
        if (valid) {
            ptr++;
            var len = Character.getNumericValue(buffer[ptr]);
            // skipping over \r\n
            ptr += 3;
            var rawData = new String(buffer, ptr, len);
            var item = deserialiseData.apply(type, rawData);
            items[index] = item;
            // skipping over \r\n to next item
            ptr += len + 2;
        }

        if (index == arrLen - 1) {
            return TailCalls.done(new BufferResult(items, ptr));
        }

        var ptrCopy = ptr;

        return TailCalls.call(() -> readBuffer(ptrCopy, buffer, arrLen, index + 1, items));
    }

    String pingCmd(RespDataType[] items) {
        return switch (items.length) {
            case 1 -> ping.get().value();
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
                yield "+PONG \"" + commandArgs + "\"\r\n";
            }
        };
    }

    void readInput(byte[] buffer, Socket clientSocket, Path aof) {
        var initialByte = (char) buffer[0];
        System.out.println(initialByte);
        boolean validType = isValidType.apply(initialByte);
        var arrLen = Character.getNumericValue(buffer[1]);

        var bufferRes = readBuffer(4, buffer, arrLen, 0, new RespDataType[arrLen]).invoke();
        var items = bufferRes.items();
        var ptr = bufferRes.ptr();

        var usedBytes = Arrays.copyOf(buffer, ptr);

        System.out.println("items:");
        Stream.of(items).forEach(System.out::println);

        var osRes = outputStream.apply(clientSocket);

        Result.of(() -> {
            var command = (bulkString) items[0];
            System.out.println("COMMAND: " + command.value());
            if (command.value().equals("PING")) {
                var res = pingCmd(items);
                if (osRes instanceof Ok<OutputStream> ok) {
                    var os = ok.value();
                    os.write(res.getBytes());
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
                .takeWhile(r -> r instanceof Ok<ByteBuffer> ok && ok.value().bytesRead() != -1)
                .map(r -> (Ok<ByteBuffer>) r)
                .map(ok -> ok.value())
                .filter(bb -> bb.bytesRead() > 6)
                .forEach(bb -> {
                    var buffer = bb.buffer();
                    readInput(buffer, clientSocket, aof);
                });

    }

    public static void main(String[] args) throws UnknownHostException, IOException {

        var app = new App();

        var aof = new Aof("aof");
        var cmds = aof.setup();
        var logs = aof.readAof(cmds);
        logs.forEach(l -> app.readInput(l.getBytes(), new Socket(), Paths.get("")));

        try (var serverSocket = new ServerSocket(6379)) {
            System.out.println("Server started, waiting for connections...");
            while (true) {
                try (var clientSocket = serverSocket.accept()) {
                    System.out.println("Client connection accepted!");
                    var is = clientSocket.getInputStream();
                    app.clientInput(is, clientSocket, Paths.get(aof.filename));
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
                System.out.println("Client disconnected.");
            }
        }
    }
}
