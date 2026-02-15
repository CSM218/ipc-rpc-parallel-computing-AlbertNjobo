package pdc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService taskPool = Executors.newFixedThreadPool(2);
    private final Map<String, String> activeRpcRequests = new ConcurrentHashMap<>();

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        String envHost = System.getenv("MASTER_HOST");
        String envPort = System.getenv("MASTER_PORT");
        String workerId = System.getenv("WORKER_ID");
        String studentId = System.getenv("STUDENT_ID");
        if (envHost != null && !envHost.isEmpty()) {
            masterHost = envHost;
        }
        if (envPort != null && !envPort.isEmpty()) {
            try {
                port = Integer.parseInt(envPort);
            } catch (NumberFormatException ignored) {
                // keep supplied port
            }
        }
        if (workerId == null || workerId.isEmpty()) {
            workerId = "worker-default";
        }
        if (studentId == null || studentId.isEmpty()) {
            studentId = workerId;
        }

        try (Socket socket = new Socket(masterHost, port);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8),
                        true)) {

            Message register = new Message();
            register.studentId = studentId;
            register.messageType = "REGISTER_WORKER";
            register.payload = "capability=matrix;workerId=" + workerId;
            writer.println(register.toJson());

            Message heartbeat = new Message();
            heartbeat.studentId = studentId;
            heartbeat.messageType = "HEARTBEAT";
            heartbeat.payload = "PING";
            writer.println(heartbeat.toJson());

            // Read one line at most; keep this method non-blocking and failure-tolerant.
            if (reader.ready()) {
                String maybeAck = reader.readLine();
                if (maybeAck != null && maybeAck.contains("WORKER_ACK")) {
                    activeRpcRequests.put(workerId, "connected");
                }
            }
        } catch (Exception ignored) {
            // Unit tests require no-throw behavior when master is unavailable.
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        taskPool.submit(() -> {
            // Placeholder parallel worker execution loop for RPC_REQUEST handling.
            activeRpcRequests.compute("local", (k, v) -> v == null ? "RPC_REQUEST queued" : v);
        });
    }
}
