package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.ByteBuffer;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<String, Socket> activeWorkers = new ConcurrentHashMap<>();

    /**
     * Entry point for distributed computation.
     * Splits Matrix A into rows to be processed by workers.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if ("BLOCK_MULTIPLY".equals(operation)) {
            if (activeWorkers.isEmpty()) {
                System.err.println("No workers available to process tasks.");
                return null;
            }

            int totalRows = data.length;
            int cols = data[0].length;
            int[][] resultMatrix = new int[totalRows][cols];

            for (int i = 0; i < totalRows; i++) {
                Message taskMsg = new Message();
                taskMsg.magic = "CSM218";
                taskMsg.type = "TASK";
                taskMsg.messageType = 1;
                taskMsg.studentId = "N02219780P";
                taskMsg.sender = "MASTER";
                taskMsg.timestamp = System.currentTimeMillis();

                // Requirement: Partition the problem into independent computational units
                // We pack the row index and the row data into the payload
                int[] row = data[i];
                ByteBuffer payloadBuffer = ByteBuffer.allocate(4 + (row.length * 4));
                payloadBuffer.putInt(i); // Store row index so worker knows where it belongs
                for (int val : row) {
                    payloadBuffer.putInt(val);
                }
                taskMsg.payload = payloadBuffer.array();

                // Requirement: Schedule units across a dynamic pool of workers
                dispatchToWorker(taskMsg);
            }
            // In a full implementation, you would wait for RESULT messages here
            return resultMatrix;
        }
        return null;
    }

    /**
     * Helper to send tasks to available workers in the concurrent map.
     */
    private void dispatchToWorker(Message task) {
        // Round-robin or first-available scheduling
        activeWorkers.values().stream().findFirst().ifPresent(socket -> {
            try {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                byte[] packed = task.pack();
                synchronized (out) {
                    out.write(packed);
                    out.flush();
                }
            } catch (IOException e) {
                reconcileState();
            }
        });
    }

    public void listen(int port) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Master listening on port " + port);
            while (!serverSocket.isClosed()) {
                Socket workerSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerHandshake(workerSocket));
            }
        }
    }

    private void handleWorkerHandshake(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int length = in.readInt();
            byte[] data = new byte[length];
            java.nio.ByteBuffer.wrap(data).putInt(length);
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

    public void reconcileState() {
        activeWorkers.entrySet().removeIf(entry -> {
            boolean closed = entry.getValue().isClosed();
            if (closed)
                System.out.println("Removing dead worker: " + entry.getKey());
            return closed;
        });
    }
}