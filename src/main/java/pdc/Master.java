package pdc;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    // Requirement: Use concurrent collections to manage workers safely
    private final ConcurrentHashMap<String, Socket> activeWorkers = new ConcurrentHashMap<>();

    /**
     * Entry point for distributed computation.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // This will be used later to split the matrix into tasks
        if ("BLOCK_MULTIPLY".equals(operation)) {
            System.out.println("Partitioning matrix for " + activeWorkers.size() + " active workers.");
        }
        return null;
    }

    /**
     * Start the communication listener using the custom Message protocol.
     */
    public void listen(int port) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Master listening on port " + port);

            while (!serverSocket.isClosed()) {
                Socket workerSocket = serverSocket.accept();
                // Requirement: Schedule units across a dynamic pool of workers
                systemThreads.submit(() -> handleWorkerHandshake(workerSocket));
            }
        }
    }

    /**
     * Handles the initial IDENTITY handshake from a Worker.
     */
    private void handleWorkerHandshake(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // 1. Read the length prefix
            int length = in.readInt();

            // 2. Create the exact size needed
            byte[] data = new byte[length];

            // 3. Put the length back into the start of the array manually
            java.nio.ByteBuffer.wrap(data).putInt(length);

            // 4. Fill the REST of the array from the stream
            in.readFully(data, 4, length - 4);

            Message msg = Message.unpack(data);

            if ("CSM218".equals(msg.magic) && "IDENTITY".equals(msg.type)) {
                activeWorkers.put(msg.studentId, socket);
                System.out.println("Worker " + msg.studentId + " registered.");
            }
        } catch (IOException e) {
            System.err.println("Handshake failed: " + e.getMessage());
        }
    }

    /**
     * System Health Check.
     */
    public void reconcileState() {
        // Requirement: Detect dead workers
        activeWorkers.entrySet().removeIf(entry -> entry.getValue().isClosed());
    }
}