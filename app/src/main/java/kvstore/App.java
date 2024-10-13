package kvstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import kvstore.Result.Ok;

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
        Function<Character, Predicate<Character>> isValidType = type -> c -> c.equals(type);

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
                                // need to setup for all the data types
                                // this is just for a bulk string
                                if (!isValidType.apply(BULK).test(initialByte)) {
                                    System.out.println("Invalid type: " + initialByte);
                                }

                                var length = Character.getNumericValue(buffer[1]);
                                System.out.println("Data: " + new String(buffer, 6, length));

                                var osRes = outputStream.apply(clientSocket);
                                if (osRes instanceof Ok<OutputStream> ok) {
                                    var os = ok.value();
                                    Result.of(() -> {
                                        os.write("+OK\r\n".getBytes());
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
