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
        var logs = read(cmds, 0, new ArrayList<String>(), new StringBuilder(), 0, 0).invoke();

        return logs;
    }

    private TailCall<ArrayList<String>> read(List<String> cmds, int currentCmd, ArrayList<String> logs,
            StringBuilder base, int cmdLen, int iterations) {
        var cmd = cmds.get(currentCmd);

        if (cmdLen != 0 || cmd.charAt(0) == '*') {
            if (cmdLen == 0) {
                System.out.println("here");
                System.out.println(cmd);
                cmdLen = Character.getNumericValue(cmd.charAt(1));
                iterations = cmdLen * 2 + 1;
            }

            if (iterations != 0) {
                base.append(cmd).append("\r\n");
                iterations--;
            }

            if (iterations == 0) {
                logs.add(base.toString());
                base.setLength(0);
                cmdLen = 0;
            }
        }

        if (currentCmd == cmds.size() - 1) {
            return TailCalls.done(logs);
        }

        final int lenCopy = cmdLen;
        final int iterCopy = iterations;

        return TailCalls.call(() -> read(cmds, currentCmd + 1, logs, base, lenCopy, iterCopy));
    }
}
