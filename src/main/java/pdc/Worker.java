package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
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
            regMsg.messageType = 0; // Type 0 for Registration/Identity
            regMsg.studentId = "N02219780P";
            regMsg.type = "IDENTITY";

            byte[] packed = regMsg.pack();
            out.write(packed);
            out.flush();

            System.out.println("Worker joined cluster. Handshake sent.");

            // Starting the execution loop in a new thread
            new Thread(this::execute).start();

        } catch (IOException e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
        }
    }

    /**
     * Executes a received task blocks asynchronously.
     */

    public void execute() {
        try {
            while (!socket.isClosed()) {
                // 1. Read the length-prefix (first 4 bytes)
                int length = in.readInt();
                byte[] data = new byte[length];

                // 2. Read the full message payload
                in.readFully(data, 4, length - 4); // Already read 4 bytes
                // Put the length back at the start for unpack()
                java.nio.ByteBuffer.wrap(data).putInt(length);

                Message task = Message.unpack(data);

                // 3. Process the task in the thread pool (Parallelism)
                threadPool.submit(() -> {
                    if ("TASK".equals(task.type)) {
                        processTask(task);
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Connection lost: " + e.getMessage());
        }
    }

    private void processTask(Message task) {
        // Simple matrix multiplication logic placeholder
        // In a real lab, you'd multiply the bytes in task.payload
        Message result = new Message();
        result.messageType = 2; // Result
        result.type = "RESULT";
        result.studentId = task.studentId;
        result.payload = task.payload; // For now, just echoing back

        try {
            synchronized (out) {
                out.write(result.pack());
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
