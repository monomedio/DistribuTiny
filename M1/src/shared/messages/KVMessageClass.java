package shared.messages;

import java.io.Serializable;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class KVMessageClass implements KVMessage, Serializable {

	private static final long serialVersionUID = 1234567L;
	private String msg;
	private String key;
	private String value;
	private StatusType status;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

    /**
     * Constructs a TextMessage object with a given array of bytes that
     * forms the message.
     *
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public KVMessageClass(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes);

		//Not nullable
		this.status = StatusType.valueOf(this.msg.substring(0, this.msg.indexOf(",")));

		//Nullable
		this.key = this.msg.substring(this.msg.indexOf(",") + 1, this.msg.lastIndexOf(","));
		this.value = this.msg.substring(this.msg.lastIndexOf(",") + 1);
	}

	/**
     * Constructs a TextMessage object with a given String that
     * forms the message.
     *
     * @param msg the String that forms the message.
     */
	public KVMessageClass(String msg) {
		this.msg = msg;
		this.msgBytes = toByteArray(msg);
	}


	/**
	 * Returns the content of this TextMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		return msg;
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

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}
	
	private byte[] addCtrChars(byte[] bytes) {
//		byte[] ctrBytes = new byte[]{LINE_FEED};
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		if (bytes[bytes.length - 1] == 13 && bytes[bytes.length - 2] == 10) {
			return bytes;
		}
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}

}
