package two_phase_lock;

import java.util.ArrayList;

// Class for the Lock Table
public class LockTable {
	String item_name;
	String lock_state;
	// Declaring array list to store the the list of transaction holding the lock
	ArrayList<String> transactionID_holding_lock = new ArrayList<String>();
	// Declaring array list to store the list of transaction waiting for a data item
	ArrayList<String> transactionID_waiting_for_lock = new ArrayList<String>();
	String lock_Status;
	int no_of_reads = 0;

	// Default Constructor
	public LockTable() {

	}

	// Parameterized Constructor
	public LockTable(String item_name, String lock_state) {
		this.item_name = item_name;
		this.lock_state = lock_state;
	}

}
