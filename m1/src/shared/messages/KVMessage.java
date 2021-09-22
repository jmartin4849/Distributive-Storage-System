package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET(0), 			/* Get - request */
		GET_ERROR(1), 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS(2), 	/* requested tuple (i.e. value) found */
		PUT(3), 			/* Put - request */
		PUT_SUCCESS(4), 	/* Put - request successful, tuple inserted */
		PUT_UPDATE(5), 	/* Put - request successful, i.e. value updated */
		PUT_ERROR(6), 		/* Put - request not successful */
		DELETE_SUCCESS(7), /* Delete - request successful */
		DELETE_ERROR(8); 	/* Delete - request successful */
		private final int value;

		/*mapping from enum to int*/
		private StatusType(int value) {
			this.value = value;
		}
		public int getValue() {
			return value;
		}

		/*caching values array because it is expensive operation*/
		private static StatusType[] values = null;
		public static StatusType fromInt(int i) {
			if(StatusType.values == null) {
				StatusType.values = StatusType.values();
			}
			return StatusType.values[i];
		}
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
	
}


