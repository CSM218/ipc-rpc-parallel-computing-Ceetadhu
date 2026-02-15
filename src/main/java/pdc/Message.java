package pdc;

import java.nio.ByteBuffer;

/**
 * Message represents the communication unit in the CSM218 protocol.
 */
public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String type;
    public int messageType;
    public String studentId = "N02219780P";
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Implements length-prefix framing to handle TCP stream boundaries.
     */
    public byte[] pack() {
        // Convert Strings to bytes
        byte[] magicBytes = (magic != null) ? magic.getBytes() : new byte[0];
        byte[] typeBytes = (type != null) ? type.getBytes() : new byte[0];
        byte[] idBytes = (studentId != null) ? studentId.getBytes() : new byte[0];
        byte[] senderBytes = (sender != null) ? sender.getBytes() : new byte[0];
        byte[] pLoad = (payload != null) ? payload : new byte[0];

        // Total Size calculation:
        // Header(4) + Magic(4+len) + Version(4) + Type(4+len) + Sender(4+len) +
        // Timestamp(8) + Payload(4+len)
        int totalSize = 4 + (4 + magicBytes.length) + 4 + (4 + typeBytes.length) + 4 + (4 + idBytes.length)
                + (4 + senderBytes.length) + 8 + (4 + pLoad.length);

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(totalSize); // Total length header
        buffer.putInt(magicBytes.length); // Magic length
        buffer.put(magicBytes); // Magic data
        buffer.putInt(version); // Version
        buffer.putInt(typeBytes.length); // Type length
        buffer.put(typeBytes); // Type data
        buffer.putInt(messageType);
        buffer.putInt(idBytes.length);
        buffer.put(idBytes);
        buffer.putInt(senderBytes.length); // Sender length
        buffer.put(senderBytes); // Sender data
        buffer.putLong(timestamp); // Timestamp
        buffer.putInt(pLoad.length); // Payload length
        buffer.put(pLoad); // Payload data

        return buffer.array();

    }

    /**
     * Reconstructing a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Message msg = new Message();

        buffer.getInt(); // Read and skip total length header

        // Read Magic
        int mLen = buffer.getInt();
        byte[] mBytes = new byte[mLen];
        buffer.get(mBytes);
        msg.magic = new String(mBytes);

        msg.version = buffer.getInt();

        // Read Type
        int tLen = buffer.getInt();
        byte[] tBytes = new byte[tLen];
        buffer.get(tBytes);
        msg.type = new String(tBytes);

        msg.messageType = buffer.getInt();

        int idLen = buffer.getInt();
        byte[] idBytes = new byte[idLen];
        buffer.get(idBytes);
        msg.studentId = new String(idBytes);

        // Read Sender
        int sLen = buffer.getInt();
        byte[] sBytes = new byte[sLen];
        buffer.get(sBytes);
        msg.sender = new String(sBytes);

        msg.timestamp = buffer.getLong();

        // Read Payload
        int pLen = buffer.getInt();
        byte[] pBytes = new byte[pLen];
        buffer.get(pBytes);
        msg.payload = pBytes;

        return msg;

    }

}