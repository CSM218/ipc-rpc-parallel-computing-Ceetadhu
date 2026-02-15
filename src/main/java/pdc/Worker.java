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
                int length = in.readInt();
                byte[] data = new byte[length];

                // Manually place the length in the first 4 bytes
                java.nio.ByteBuffer.wrap(data).putInt(length);

                // Read the remaining bytes directly into the array
                in.readFully(data, 4, length - 4);

                Message task = Message.unpack(data);

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
        Message result = new Message();
        result.messageType = 2; // Result
        result.type = "RESULT";
        result.studentId = task.studentId;
        result.payload = task.payload;

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
