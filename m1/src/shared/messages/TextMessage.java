package shared.messages;

import java.io.Serializable;
import java.util.Arrays;

public class TextMessage implements Serializable, KVMessage {

    private static final long serialVersionUID = 5549512212003782618L;
    private KVMessage.StatusType status;
    private String value;
    private String key;
    private byte[] msgBytes;

    /**
     * Constructs a TextMessage object with a given array of bytes that
     * forms the message.
     *
     * @param bytes the bytes that form the message in ASCII coding.
     */
    public TextMessage(byte[] bytes) {
        this.msgBytes = bytes;
        this.status = KVMessage.StatusType.fromInt(bytes[0]);
        this.key = new String(Arrays.copyOfRange(bytes, 1, 20)).trim();
        this.value = new String(Arrays.copyOfRange(bytes, 21, bytes.length));
    }

    /**
     * Constructs a TextMessage object with a given StatusType, key and value.
     *
     * @param status StatusType enum representing type of message.
     * @param key Key of KV-pair related to message.
     * @param value Value of KV-pair related to message.
     */
    public TextMessage(KVMessage.StatusType status, String key, String value){
        this.status = status;
        this.key = key;
        this.value = value;
        byte[] tmp;
        if(value == null){
            tmp = new byte[21];
            tmp[0] = (byte) status.getValue();
            byte[] keyBytes = key.getBytes();
            System.arraycopy(keyBytes, 0, tmp, 1, keyBytes.length);
        } else {
            this.value = value;
            tmp = new byte[(21 + value.length())];
            tmp[0] = (byte) status.getValue();
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.getBytes();
            System.arraycopy(keyBytes, 0, tmp, 1, keyBytes.length);
            System.arraycopy(valueBytes, 0, tmp, 21, valueBytes.length);
        }
        this.msgBytes = addCtrChars(tmp);
    }

    /**
     * Returns Value in KV-pair being sent in message.
     *
     * @return Value contained in TextMessage
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns Key in KV-pair being sent in message.
     *
     * @return Key contained in TextMessage
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns StatusType representing type of TextMessage.
     *
     * @return StatusType contained in TextMessage
     */
    public KVMessage.StatusType getStatus() {
        return status;
    }

    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     *
     * @return the content of this message as an array of bytes
     * 		in ASCII coding.
     */
    public byte[] getMsgBytes() {
        return msgBytes;
    }

    /**
     * Adding Control Characters to an array of bytes to make format suitable for
     * sending and receiving over socket.
     *
     * @param bytes Array of bytes to format.
     * @return Array of bytes with end of line characters appended.
     */
    public static byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{0x0A, 0x0D};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }
}
