package kvstore;

import java.util.concurrent.Callable;
import java.util.function.Function;

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
}
