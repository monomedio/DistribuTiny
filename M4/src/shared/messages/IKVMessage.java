package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		GET, 			/* GET - REQUEST */
		GET_ERROR, 		/* REQUESTED TUPLE (I.E. VALUE) NOT FOUND */
		GET_SUCCESS, 	/* REQUESTED TUPLE (I.E. VALUE) FOUND */
		PUT, 			/* PUT - REQUEST */
		PUT_SUCCESS, 	/* PUT - REQUEST SUCCESSFUL, TUPLE INSERTED */
		PUT_UPDATE, 	/* PUT - REQUEST SUCCESSFUL, I.E. VALUE UPDATED */
		PUT_ERROR, 		/* PUT - REQUEST NOT SUCCESSFUL */
		DELETE_SUCCESS, /* DELETE - REQUEST SUCCESSFUL */
		DELETE_ERROR, 	/* DELETE - REQUEST NOT SUCCESSFUL */
		FAILED,			/* ALL OTHER CASES*/
		SERVER_NOT_RESPONSIBLE, /* WHEN KEY NOT IN SERVER RANGE DO NOT RETURN ERROR, TRY RETRY */
		SERVER_WRITE_LOCK, /* CAN ONLY SERVE GET REQUESTS */
		SERVER_STOPPED,  /* SERVER NOT BOOTSTRAPPED BY ECS YET */
		KEYRANGE_SUCCESS, /* MESSAGE CONTAINS METADATA */
		TR_REQ, /* INITIATING DATA TRANSFER*/
		TR_RES, /* TRANSFER DATA RESPONSE */
		TR_INIT, /* INITIALIZE WRITING DATA INTO NEW SERVER */
		TR_SUCC, /* SUCCESSFULLY WRITTEN DATA INTO NEW SERVER*/
		META_UPDATE, /* UPDATE METADATA */
		SHUTDOWN, /*SENT BY ECSLISTENER WHEN KVSERVER SHUTS DOWN*/
		LAST_ONE, /*SENT BY ECS TO LISTENER WHEN THE SERVER REQUESTING SHUTDOWN IS THE LAST ONE*/
		SERV_INIT, /* MESSAGE FROM INITIALIZING SERVER CONTAINING ITS CLIENT LISTENER IP AND PORT AS KEY AND VALUE RESPECTIVELY */
		KEYRANGE,  /*SENT BY CLIENT TO REQUEST UPDATED METADATA FROM A KVSERVER*/
		PUT_R, GET_R, REPLICATE, KEYRANGE_READ, KEYRANGE_READ_SUCCESS, QUIET_KEYRANGE, BROADCAST_UPDATE, BROADCAST_DELETE
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


