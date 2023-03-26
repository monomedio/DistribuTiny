package shared.messages;

public interface IKVMessage {
	
	public enum StatusType {
		get, 			/* GET - REQUEST */
		get_error, 		/* REQUESTED TUPLE (I.E. VALUE) NOT FOUND */
		get_success, 	/* REQUESTED TUPLE (I.E. VALUE) FOUND */
		put, 			/* PUT - REQUEST */
		put_success, 	/* PUT - REQUEST SUCCESSFUL, TUPLE INSERTED */
		put_update, 	/* PUT - REQUEST SUCCESSFUL, I.E. VALUE UPDATED */
		put_error, 		/* PUT - REQUEST NOT SUCCESSFUL */
		delete_success, /* DELETE - REQUEST SUCCESSFUL */
		delete_error, 	/* DELETE - REQUEST NOT SUCCESSFUL */
		failed,			/* ALL OTHER CASES*/
		server_not_responsible, /* WHEN KEY NOT IN SERVER RANGE DO NOT RETURN ERROR, TRY RETRY */
		server_write_lock, /* CAN ONLY SERVE GET REQUESTS */
		server_stopped,  /* SERVER NOT BOOTSTRAPPED BY ECS YET */
		keyrange_success, /* MESSAGE CONTAINS METADATA */
		tr_req, /* INITIATING DATA TRANSFER*/
		tr_res, /* TRANSFER DATA RESPONSE */
		tr_init, /* INITIALIZE WRITING DATA INTO NEW SERVER */
		tr_succ, /* SUCCESSFULLY WRITTEN DATA INTO NEW SERVER*/
		meta_update, /* UPDATE METADATA */
		shutdown, /*SENT BY ECSLISTENER WHEN KVSERVER SHUTS DOWN*/
		last_one, /*SENT BY ECS TO LISTENER WHEN THE SERVER REQUESTING SHUTDOWN IS THE LAST ONE*/
		serv_init, /* MESSAGE FROM INITIALIZING SERVER CONTAINING ITS CLIENT LISTENER IP AND PORT AS KEY AND VALUE RESPECTIVELY */
		keyrange,  /*SENT BY CLIENT TO REQUEST UPDATED METADATA FROM A KVSERVER*/
		put_r, get_r, replicate, keyrange_read, keyrange_read_success,
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


