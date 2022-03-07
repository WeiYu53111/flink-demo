package sql;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.local.LocalContextUtils;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.Optional;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2022/3/6 0006
 */
public class Demo2 {

    public static void main(String[] args) {
        String[] params = null;
        final CliOptions options =  new CliOptions(
                false,
                null,
                null,
                null,
                new ArrayList<>(),
                null,
                null,
                null,
                null);
        DefaultContext defaultContext = LocalContextUtils.buildDefaultContext(options);
        final Executor executor = new LocalExecutor(defaultContext);
        executor.start();
        String sessionId = executor.openSession(options.getSessionId());
        try {
           /* // add shutdown hook
            Runtime.getRuntime()
                    .addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));
*/
            // do the actual work
            final Optional<Operation> operation = parseCommand(executor,"wy","select 1");

            if(!operation.isPresent()){
                throw new RuntimeException("sql 解析错误");
            }
            TableResult result = executor.executeOperation(sessionId, operation.get());
            result.print();
        } finally {
            executor.closeSession(sessionId);
        }
    }

    private static Optional<Operation> parseCommand(Executor executor,String sessionId,String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        // meet bad case, e.g ";\n"
        if (stmt.trim().isEmpty()) {
            return Optional.empty();
        }

        Operation operation = executor.parseStatement(sessionId, stmt);
        return Optional.of(operation);
    }

    private static class EmbeddedShutdownThread extends Thread {

        private final String sessionId;
        private final Executor executor;

        public EmbeddedShutdownThread(String sessionId, Executor executor) {
            this.sessionId = sessionId;
            this.executor = executor;
        }

        @Override
        public void run() {
            // Shutdown the executor
            System.out.println("\nShutting down the session...");
            executor.closeSession(sessionId);
            System.out.println("done.");
        }
    }

}
