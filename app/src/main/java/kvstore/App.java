package kvstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import kvstore.Result.Ok;
import kvstore.respDataType.bulkString;
import kvstore.respDataType.respArray;
import kvstore.respDataType.respInteger;
import kvstore.respDataType.simpleError;
import kvstore.respDataType.simpleString;

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
    };

}

public class App {
    static final char STRING = '+';
    static final char ERROR = '-';
    static final char INTEGER = ':';
    static final char BULK = '$';
    static final char ARRAY = '*';

    record value(String typ, String str, int num, String bulk, value[] array) {
    };

    public static void main(String[] args) throws UnknownHostException, IOException {
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

        try (var serverSocket = new ServerSocket(6379)) {
            System.out.println("Server started, waiting for connections...");
            while (true) {
                try (var clientSocket = serverSocket.accept()) {
                    System.out.println("Client connection accepted!");
                    var is = clientSocket.getInputStream();

                    Stream.generate(() -> readBytes.apply(is))
                            .takeWhile(r -> r instanceof Ok<byteBuffer> ok && ok.value().bytesRead() != -1)
                            .map(r -> (Ok<byteBuffer>) r)
                            .map(ok -> ok.value())
                            .filter(bb -> bb.bytesRead() > 6)
                            .forEach(bb -> {
                                var buffer = bb.buffer();

                                var initialByte = (char) buffer[0];
                                boolean validType = isValidType.apply(initialByte);
                                var arrLen = Character.getNumericValue(buffer[1]);
                                var items = new respDataType[arrLen];

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

                                System.out.println("items:");
                                Stream.of(items).forEach(System.out::println);

                                var osRes = outputStream.apply(clientSocket);
                                if (osRes instanceof Ok<OutputStream> ok) {
                                    var os = ok.value();

                                    Result.of(() -> {
                                        switch (validType) {
                                            case true -> os.write("+OK\r\n".getBytes());
                                            case false -> os.write("-INVALID TYPE\r\n".getBytes());
                                        }
                                        return true;
                                    });
                                }
                            });

                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
                System.out.println("Client disconnected.");
            }
        }
    }
}
