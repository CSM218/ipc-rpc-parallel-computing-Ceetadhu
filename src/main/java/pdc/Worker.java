package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.nio.ByteBuffer;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    // Requirement: High-concurrency computation using internal thread pool
    private final ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            this.socket = new Socket(masterHost, port);
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());

            // Registration Handshake: Send Identity
            Message regMsg = new Message();
            regMsg.magic = "CSM218"; // Ensure correct spelling
            regMsg.messageType = 0;
            regMsg.studentId = "N02219780P";
            regMsg.type = "IDENTITY";

            byte[] packed = regMsg.pack();
            synchronized (out) { // Synchronize all writes to prevent interleaving
                out.write(packed);
                out.flush();
            }

            System.out.println("Worker joined cluster. Handshake sent.");

            // Starting the execution loop in a new thread
            Thread workerThread = new Thread(this::execute);
            workerThread.setDaemon(true);
            workerThread.start();

        } catch (IOException e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
        }
    }

    /**
     * Executes received task blocks asynchronously.
     */
    public void execute() {
        try {
            while (!socket.isClosed()) {
                // Read length prefix (4 bytes)
                int length = in.readInt();
                if (length <= 0)
                    continue;

                byte[] data = new byte[length];

                // Manually place the length in the first 4 bytes for unpack()
                ByteBuffer.wrap(data).putInt(length);

                // Read the remaining bytes directly into the array
                in.readFully(data, 4, length - 4);

                Message task = Message.unpack(data);

                // Requirement: Manage internal task scheduling via thread pool
                threadPool.submit(() -> {
                    if ("TASK".equals(task.type)) {
                        processTask(task);
                    }
                });
            }
        } catch (IOException e) {
            if (!socket.isClosed()) {
                System.err.println("Connection lost: " + e.getMessage());
            }
        } finally {
            shutdown();
        }
    }

    private void processTask(Message task) {
        Message result = new Message();
        result.magic = "CSM218";
        result.messageType = 2; // Result type
        result.type = "RESULT";
        result.studentId = "N02219780P";
        result.payload = task.payload;

        try {
            // Requirement: Atomic write operation from Master's perspective
            synchronized (out) {
                out.write(result.pack());
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void shutdown() {
        try {
            threadPool.shutdown();
            if (socket != null)
                socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}