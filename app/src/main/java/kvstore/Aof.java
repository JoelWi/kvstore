package kvstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Aof {
    public final String filename;

    public Aof(String filename) {
        this.filename = filename;
    }

    public List<String> setup() throws IOException {
        var path = Paths.get(filename);
        return switch (Files.exists(path)) {
            case true -> Files.readAllLines(path);
            case false -> List.of("");
        };
    }

    public List<String> readAof(List<String> cmds) {
        return read(cmds, 0, new ArrayList<String>(), new StringBuilder(), 0).invoke();
    }

    private TailCall<ArrayList<String>> read(List<String> cmds, int index, ArrayList<String> logs,
            StringBuilder base, int iterations) {
        var cmd = cmds.get(index);

        if (cmd.charAt(0) == '*') {
            iterations = Character.getNumericValue(cmd.charAt(1)) * 2 + 1;
        }

        switch (iterations) {
            case 1 -> {
                base.append(cmd).append("\r\n");
                logs.add(base.toString());
                base.setLength(0);
                iterations = 0;
            }
            default -> {
                base.append(cmd).append("\r\n");
                iterations--;
            }
        }

        if (index == cmds.size() - 1) {
            return TailCalls.done(logs);
        }

        final int iterCopy = iterations;
        return TailCalls.call(() -> read(cmds, index + 1, logs, base, iterCopy));
    }
}
