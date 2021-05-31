package two_phase_lock;

import java.util.ArrayList;

//Class for the transaction table
public class Transaction_Table {

	String transaction_ID;
	int timeStamp;
	String transaction_State;

	// Declaring array list to store list of items locked
	ArrayList<String> list_of_items_locked = new ArrayList<String>();

	public void addBeginRecord(String transID, int time) {
		transaction_ID = transID;
		timeStamp = time;
		transaction_State = "Active";

	}

}
