package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request not successful */
		FAILED,			/* All other cases*/
		SERVER_NOT_RESPONSIBLE, /* When key not in server range DO NOT RETURN ERROR, TRY RETRY */
		SERVER_WRITE_LOCK, /* Can only serve get requests */
		SERVER_STOPPED,  /* Server not bootstrapped by ECS yet */
		KEYRANGE_SUCCESS, /* Message contains metadata */
		TR_REQ, /* Initiating data transfer*/
		TR_RES, /* Transfer data response */
		TR_INIT, /* Initialize writing data into new server */
		TR_SUCC, /* Successfully written data into new server*/
		META_UPDATE, /* Update metadata */
		SHUTDOWN, /*Sent by ECSListener when KVServer shuts down*/
		LAST_ONE, /*Sent by ECS to listener when the server requesting shutdown is the last one*/
		SERV_INIT /* Message from initializing server containing its client listener IP and port as key and value respectively */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * @return a UTF-8 encoded byte array terminated by \r\n that represents the message
	 */
	public byte[] getMessageBytes();

	/**
	 * @return a String representation of the message, NOT terminated by \r\n
	 */
	public String getMessage();
	
}


