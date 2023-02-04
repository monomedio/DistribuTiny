package shared.messages;

public class KVMessage implements IKVMessage {

    private String key;
    private String value;
    private StatusType status;

    private byte[] messageBytes;

    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    /**
     * Constructor for KVMessage
     * @param status the request status carried by a message
     * @param key the key carried by a message
     * @param value the value carried by a message
     */
    public KVMessage(StatusType status, String key, String value) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.messageBytes = toByteArray(status.toString() + " " + key + " " + value);
    }

    /**
     * Constructor for KVMessage that represents a 2-ary command
     * @param status the request status carried by a message
     * @param key the key carried by a message
     */
    public KVMessage(StatusType status, String key) {
        this.status = status;
        this.key = key;
        this.messageBytes = toByteArray(status.toString() + " " + key);
    }

    /**
     * Constructor for KVMessage from a byte array
     * @param messageBytes a UTF-8 encoded byte array representing the message
     */
    public KVMessage(byte[] messageBytes) {
        this.messageBytes = messageBytes;
        String[] argsArray = messageBytesToArgsArray(messageBytes);
        this.status = StatusType.valueOf(argsArray[0]);
        this.key = argsArray[1];

        if (argsArray.length == 3) {
            this.value = argsArray[2];
        }
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.status;
    }

    @Override
    public byte[] getMessageBytes() {
        return this.messageBytes;
    }

    @Override
    public String getMessage() {
        return new String(this.messageBytes);
    }

//////////////////// METHOD HELPERS ////////////////////

    private String[] messageBytesToArgsArray(byte[] messageBytes) {
        // Parses byte array into String using UTF-8 charset
        String temp = new String(messageBytes);
        // Splits into arguments, applying space delimiter at most twice
        // https://stackoverflow.com/questions/24748619/split-string-by-whitespaces-removes-new-line-characters
        String[] args = temp.split("[ \\t\\x0B\\f]+", 3);
        return args;
    }

    private byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{RETURN, LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }
}
