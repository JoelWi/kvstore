package kvstore;

sealed interface RespDataType {
    record simpleString(String value) implements RespDataType {
    };

    record simpleError(String value) implements RespDataType {
    };

    record respInteger(int value) implements RespDataType {
    };

    record bulkString(String value) implements RespDataType {
    };

    record respArray(String value) implements RespDataType {
    }

}
