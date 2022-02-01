package shared.messages;

import java.io.Serializable;
import java.util.Objects;

import exceptions.InvalidMessageException;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class KVMessage implements Serializable, IKVMessage {

	private static final long serialVersionUID = 5549512212003782618L;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;	
	private static final int MAX_KEY = 20;	
	private static final int MAX_VALUE = 20000;	
	private static final int MAX_STATUS = 1;	
	
	private String key;
	private String value;
	private StatusType status;
	private byte[] msgBytes;
	
	
    /**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public KVMessage(byte[] bytes) throws InvalidMessageException {
		this.msgBytes = addCtrChars(bytes);
		// Deserialize the data
		this.status = StatusType.parse(msgBytes[0]);
		
		if (bytes.length > MAX_STATUS) {
			byte[] keyBytes = new byte[MAX_KEY];
			System.arraycopy(msgBytes, MAX_STATUS, keyBytes, 0, MAX_KEY);
			this.key = new String(keyBytes).trim();
			
			if (this.key.getBytes().length > MAX_VALUE) throw new InvalidMessageException("Invalid key length!");
			
			if (bytes.length > (MAX_STATUS + MAX_KEY)) {
				byte[] valueBytes = new byte[MAX_VALUE];
				System.arraycopy(msgBytes, MAX_STATUS + MAX_KEY, valueBytes, 0, msgBytes.length - (MAX_STATUS + MAX_KEY));
				this.value = new String(valueBytes).trim();
				
				if (this.value.getBytes().length > MAX_VALUE) throw new InvalidMessageException("Invalid key length!");
			} else {
				this.value = null;
			}
		}
		else {
			this.key = null;
		}
	}
	
	/**
     * Constructs a TextMessage object with the provided args. 
     * 
     * @param key the key
     * @param value the value
     * @param status the status
     */
	public KVMessage(String key, String value, StatusType status) throws InvalidMessageException {
		this.key = key;
		if (this.key != null && this.key.getBytes().length > MAX_KEY)
			throw new InvalidMessageException("Invalid key length!");

		this.value = value;
		if (this.value != null && this.value.getBytes().length > MAX_VALUE) 
			throw new InvalidMessageException("Invalid key length!");

		this.status = status;
		// Serialize the data
		this.msgBytes = toByteArray(key, value, status);
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
	public String print() {
		StringBuilder res = new StringBuilder();
		if (status != null) res.append("Status=" + status);
		if (key != null) res.append(",Key=" + key);
		if (value != null) res.append(",Value=" + value);

		return res.length() > 0 ? res.toString() : "<NULL>";
	}

	@Override
	public boolean equal(IKVMessage other) {
		if (this.getStatus() != other.getStatus()) return false;
		if (!Objects.equals(this.getKey(), other.getKey())) return false;
		if (!Objects.equals(this.getValue(), other.getValue())) return false;

		return true;
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
	
	private byte[] addCtrChars(byte[] bytes) {
		// byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] ctrBytes = new byte[]{LINE_FEED};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	// TODO: Try and overflow the key/value and see what happens (put checks to restrict this)
	// Serialized Structure:
	// Value must be at the end just in case its null
	// [Status][Key][  Value   ]
	//    1	    20   120 kByte
	private byte[] toByteArray(String key, String value, StatusType status) {
		byte[] msgBytes;
		if (value == null) {
			msgBytes = new byte[MAX_STATUS + MAX_KEY];
		} else {
			byte[] valueBytes = value.getBytes();
			msgBytes = new byte[MAX_STATUS + MAX_KEY + valueBytes.length];
			System.arraycopy(valueBytes, 0, msgBytes, MAX_STATUS + MAX_KEY, valueBytes.length);
		}
		msgBytes[0] = status.getVal();
		
		byte[] keyBytes = key.getBytes();
		System.arraycopy(keyBytes, 0, msgBytes, MAX_STATUS, keyBytes.length);
		
		return addCtrChars(msgBytes);		
	}
}
