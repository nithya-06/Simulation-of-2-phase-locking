package two_phase_lock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

// Main Class for the Rigorous 2 Phase locking
public class Rigorous_2PL {

	// Declaring Hash Map for the Transaction Table
	public static HashMap<String, Transaction_Table> transactionTablehashmap = new HashMap<String, Transaction_Table>();
	// Declaring Hash Map for the Lock Table
	public static HashMap<String, LockTable> locktablehashmap = new HashMap<String, LockTable>();
	// Declaring ArrayList for the List of operations waiting
	public static ArrayList<String> list_of_operation_waiting = new ArrayList<String>();
	// Declaring ArrayList for the data item to process when transaction is
	// committed
	public static ArrayList<String> data_item_to_process = new ArrayList<String>();

	// Reading the text from the given input file
	public void readFile() throws IOException, FileNotFoundException, StringIndexOutOfBoundsException {

		// Declaring a string to store the each text from the input file
		String text;
		// Initializing the time stamp for the transactions
		int timeStamp = 0;
		try {
			// Converting the file into file object
			File file = new File(
					"C:/Users/bnith/Downloads/input1.txt");
			// Reading the text from the given file
			BufferedReader read = new BufferedReader(new FileReader(file));
			// Reading every text from the given input file
			while ((text = read.readLine()) != null) {
				if (text.charAt(0) == 'b') {
					System.out.println(text);
					begin_transaction("T" + text.substring(1, 2), ++timeStamp);
				} else if (text.charAt(0) == 'r') {
					System.out.println(text);
					read_lock("T" + text.substring(1, 2), text.substring(text.indexOf('(') + 1, text.indexOf(')')));
				} else if (text.charAt(0) == 'w') {
					System.out.println(text);
					write_lock("T" + text.substring(1, 2), text.substring(text.indexOf('(') + 1, text.indexOf(')')));
				} else if (text.charAt(0) == 'e') {
					System.out.println(text);
					end_transaction("T" + text.charAt(1));
				}

			}
			read.close();
		}

		catch (StringIndexOutOfBoundsException e) {
			System.out.println(e);
		}

	}

	// Method to implement inserting a new record for a transaction in the
	// transaction table
	public static void begin_transaction(String transactionID, int timeStamp) {
		Transaction_Table t_beginTransaction = new Transaction_Table();
		t_beginTransaction.addBeginRecord(transactionID, timeStamp);
		transactionTablehashmap.put(transactionID, t_beginTransaction);
		System.out.println("Begin Transaction" + " " + transactionID + " " + ". Record is added to transaction table. " + transactionID + " " + "state = active.");

	}

	// Method to implement read lock on a given data item by a transaction
	public static void read_lock(String transactionID, String data_item) {
		Transaction_Table tt_readLock = transactionTablehashmap.get(transactionID);
		// To check if the transaction is active
		if (tt_readLock.transaction_State.equals("Active")) {

			// Lock down grade from write lock to read lock
			if (locktablehashmap.containsKey(data_item)) {
				LockTable lock_readLock = locktablehashmap.get(data_item);

				if (lock_readLock.lock_state.equals("write-locked")
						&& lock_readLock.transactionID_holding_lock.contains(transactionID)) {
					lock_readLock.transactionID_holding_lock.clear();
					lock_readLock.transactionID_holding_lock.add(transactionID);
					lock_readLock.lock_state = "read-locked";
					lock_readLock.lock_Status = "Locked";
					tt_readLock.list_of_items_locked.add(data_item);
					System.out.println(transactionID + " " + "has been down graded from write lock to read lock on "
							+ " " + data_item);

				}
				// If the data item is not locked, Transaction acquires read lock on the
				// requesting data item
				else if (lock_readLock.lock_Status.equals("Unlocked")) {
					lock_readLock.item_name = data_item;
					lock_readLock.lock_state = "read-locked";
					lock_readLock.transactionID_holding_lock.add(transactionID);
					lock_readLock.lock_Status = "Locked";
					tt_readLock.list_of_items_locked.add(data_item);
					transactionTablehashmap.put(tt_readLock.transaction_ID, tt_readLock);
					System.out.println(data_item +" " + "read locked by" + " " + transactionID + " " + ": Lock table record for " + " " + data_item + " " + " is created with mode R(" + " " + transactionID + " " + ") holds lock.");

				}
				// To check for Read Read situation of other Transaction ID
				else if (lock_readLock.lock_state.equals("read-locked")) {
					lock_readLock.transactionID_holding_lock.add(tt_readLock.transaction_ID);
					tt_readLock.list_of_items_locked.add(data_item);
					transactionTablehashmap.put(tt_readLock.transaction_ID, tt_readLock);
					lock_readLock.no_of_reads = lock_readLock.no_of_reads + 1;
					System.out.println(data_item + " read locked by " + transactionID + " :Lock table record for Y is updated. " + " ");

				}
				// To check for Write Read Conflict
				else if (lock_readLock.lock_state.equals("write-locked")) {
					if (!lock_readLock.transactionID_holding_lock.contains(transactionID)) {
						String holding_lock = lock_readLock.transactionID_holding_lock.get(0);
						Transaction_Table twr = transactionTablehashmap.get(holding_lock);
						// Implementing Wound Wait Protocol based on time stamp of the transactions
						if (twr.timeStamp > tt_readLock.timeStamp) {
							twr.transaction_State = "Aborted";
							transactionTablehashmap.put(twr.transaction_ID, twr);
							lock_readLock.transactionID_holding_lock.clear();
							lock_readLock.transactionID_holding_lock.add(tt_readLock.transaction_ID);
							lock_readLock.lock_state = "read-locked";
							if (!tt_readLock.list_of_items_locked.contains(data_item)) {
								tt_readLock.list_of_items_locked.add(data_item);
								transactionTablehashmap.put(tt_readLock.transaction_ID, tt_readLock);
								System.out.println(twr.transaction_ID + " "
										+ "is aborted based on wound wait protocol and" + " "
										+ tt_readLock.transaction_ID + " " + "acquires read lock on" + " " + data_item);
								abort_transaction(twr);
							}

						} else {
							String list_of_waiting_operation = "r" + transactionID.charAt(1) + "(" + data_item + ")";
							tt_readLock.transaction_State = "Blocked";
							list_of_operation_waiting.add(list_of_waiting_operation);
							lock_readLock.transactionID_waiting_for_lock.add(tt_readLock.transaction_ID);
							System.out
									.println(transactionID + " " + "is waiting as per wound wait protocol on data item"
											+ " " + data_item + " based on higher timestamp");

						}
					}
				}
			}
			// Creating a record for read lock on a data item in the lock table
			else {

				LockTable lock_newReadLock = new LockTable(data_item, "read-locked");
				lock_newReadLock.transactionID_holding_lock.add(transactionID);
				lock_newReadLock.lock_Status = "Locked";
				//System.out.println(transactionID + " " + "acquires a read lock on" + " " + data_item);
				System.out.println(data_item +" " + "read locked by" + " " + transactionID + " " + ": Lock table record for " + " " + data_item + " " + " is created with mode R(" + " " + transactionID + " " + ") holds lock.");
				lock_newReadLock.no_of_reads = lock_newReadLock.no_of_reads + 1;
				locktablehashmap.put(data_item, lock_newReadLock);
				if (!tt_readLock.list_of_items_locked.contains(data_item)) {
					tt_readLock.list_of_items_locked.add(data_item);
					transactionTablehashmap.put(tt_readLock.transaction_ID, tt_readLock);
					

				}
			}
		}
		// To check if the transaction is blocked
		else if (tt_readLock.transaction_State.equals("Blocked")) {
			if (locktablehashmap.containsKey(data_item)) {
				LockTable lock_blockedTransaction = locktablehashmap.get(data_item);
				lock_blockedTransaction.transactionID_waiting_for_lock.add(transactionID);
				locktablehashmap.put(data_item, lock_blockedTransaction);

			}
			if (!tt_readLock.list_of_items_locked.contains(data_item)) {
				tt_readLock.list_of_items_locked.add(data_item);
				String list_of_waiting_operation = "r" + transactionID.charAt(1) + "(" + data_item + ")";
				list_of_operation_waiting.add(list_of_waiting_operation);
				System.out.println("Because " + transactionID + " " + " is in blocked state and operation" + " "
						+ list_of_waiting_operation + " " + "is added to the list of operation waiting queue");

			}

		}
		// To check if the transaction is aborted
		else if (tt_readLock.transaction_State.equals("Aborted")) {
			System.out.println(transactionID + " " + "is aborted");

		}
	}

	// Method to implement write lock on a data item by a transaction
	public static void write_lock(String transactionID, String data_item) {
		Transaction_Table tt_writeLock = transactionTablehashmap.get(transactionID);
		if (transactionTablehashmap.containsKey(transactionID)) {

			// Check if the transaction is Active
			if (tt_writeLock.transaction_State.equals("Active")) {

				// Check if the data item is already present in the Lock table
				if (!(locktablehashmap.containsKey(data_item))) {
					LockTable l_writeLock = new LockTable(data_item, "write-locked");
					l_writeLock.transactionID_holding_lock.add(transactionID);
					l_writeLock.lock_Status = "Locked";
					System.out.println(transactionID + " " + "acquires a write lock on" + " " + data_item);
					if (!tt_writeLock.list_of_items_locked.contains(data_item)) {
						tt_writeLock.list_of_items_locked.add(data_item);
						transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);
						locktablehashmap.put(data_item, l_writeLock);
					}
				} else if (locktablehashmap.containsKey(data_item)) {

					LockTable lock1 = locktablehashmap.get(data_item);
					ArrayList<String> tID_holding_lock = new ArrayList<String>();
					tID_holding_lock = lock1.transactionID_holding_lock;
					int read_list_size = tID_holding_lock.size();
					int requesting_Trans_TS = tt_writeLock.timeStamp;
					int holding_Trans_TS;

					// If the data item is not locked, Transaction acquires write lock
					if (lock1.lock_Status.equals("Unlocked")) {
						lock1.item_name = data_item;
						lock1.lock_state = "write-locked";
						lock1.transactionID_holding_lock.add(transactionID);
						lock1.lock_Status = "Locked";
						tt_writeLock.list_of_items_locked.add(data_item);
						System.out.println(transactionID + " " + "acquires write lock on" + " " + data_item);

					} else if (lock1.lock_state == "read-locked") {

						// Check if only one transaction is reading the data item
						if (read_list_size == 1) {

							String read_Lock_tid = tID_holding_lock.get(0);

							// check if both the requesting transactions and transaction holding lock are
							// same,
							// then upgrade from read lock to write lock
							// Lock Upgrade
							if (read_Lock_tid.equals(transactionID)) {
								lock1.lock_state = "write-locked";
								lock1.no_of_reads = 0;
								System.out.println("Read lock upgraded to write lock for item" + " " + data_item + " by " + transactionID + ",lock table updated to mode W.");
							}
							// else conflict occurs and implement wound wait protocol
							else {
								Transaction_Table trans_holding = transactionTablehashmap.get(read_Lock_tid);
								holding_Trans_TS = trans_holding.timeStamp;
								// Implementing Wound Wait Protocol
								if (requesting_Trans_TS < holding_Trans_TS) {

									abort_transaction(trans_holding);
									lock1.lock_state = "write-locked";
									lock1.transactionID_holding_lock.add(transactionID);
									lock1.lock_Status = "Locked";
									if (!tt_writeLock.list_of_items_locked.contains(data_item)) {
										tt_writeLock.list_of_items_locked.add(data_item);
										transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);
									}
									System.out.println(transactionID + " acquires write lock on " + data_item);

								} else {
									String waiting_operation = "w" + transactionID.charAt(1) + "(" + data_item + ")";
									list_of_operation_waiting.add(waiting_operation);
									tt_writeLock.transaction_State = "Blocked";
									lock1.transactionID_waiting_for_lock.add(transactionID);
									transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);

									System.out.println(transactionID + " "
											+ "has been blocked as per wound wait protocol for the data item " + " "
											+ data_item + " based on the higher timestamp");
								}
							}

						}
						// To check if there are many transactions reading a data item and a transaction
						// requests for
						// write lock on the same data item
						else if (read_list_size > 1) {

							ArrayList<String> tID_holding_lock1 = new ArrayList<String>();
							ArrayList<Integer> tS_holding_locks = new ArrayList<Integer>();
							tID_holding_lock1 = lock1.transactionID_holding_lock;
							HashMap<Integer, String> timeStampTablehashmap = new HashMap<Integer, String>();
							for (int i = 0; i < tID_holding_lock1.size(); i++) {

								String Trans_in_list = tID_holding_lock1.get(i);
								Transaction_Table transaction = transactionTablehashmap.get(Trans_in_list);
								String tID = transaction.transaction_ID;
								Integer tS = transaction.timeStamp;
								tS_holding_locks.add(tS);
								timeStampTablehashmap.put(tS, tID);
							}

							Collections.sort(tS_holding_locks);

							int first_TS = tS_holding_locks.get(0);
							// To check if the transactions are same, then Lock upgrade
							if (requesting_Trans_TS == first_TS) {
								lock1.lock_state = "write-locked";
								lock1.no_of_reads = 0;
								System.out.println(transactionID + " "
										+ "has been upgraded from read-lock to write-lock on " + data_item);
								for (int j = 1; j < tS_holding_locks.size(); j++) {
									int TS_get = tS_holding_locks.get(j);
									String trans_to_abort = timeStampTablehashmap.get(TS_get);
									Transaction_Table abortTrans = transactionTablehashmap.get(trans_to_abort);

									abort_transaction(abortTrans);
								}

							} else {
								String status = " ";
								for (int k = 0; k < tS_holding_locks.size(); k++) {
									int holding_timeStamp = tS_holding_locks.get(k);

									if (requesting_Trans_TS < holding_timeStamp) {

										String trans_to_abort = timeStampTablehashmap.get(holding_timeStamp);
										Transaction_Table abortTrans = transactionTablehashmap.get(trans_to_abort);

										abort_transaction(abortTrans);
									} else if (requesting_Trans_TS > holding_timeStamp) {
										status = "Wait";
									}

								}

								if (status.equals("Wait")) {
									String waiting_operation = "w" + transactionID.charAt(1) + "(" + data_item + ")";
									list_of_operation_waiting.add(waiting_operation);
									tt_writeLock.transaction_State = "Blocked";
									if (!lock1.transactionID_waiting_for_lock.contains(transactionID)) {
										lock1.transactionID_waiting_for_lock.add(transactionID);
										System.out.println(data_item + " is read locked by a list of transactions" + transactionID + " "
												+ "has been blocked as per wound wait protocol for the data item " + " "
												+ data_item + " based on higher timestamp.");
									}
									transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);

								} else if (status == " ") {
									lock1.lock_state = "write-locked";
									lock1.transactionID_holding_lock.add(transactionID);
									lock1.lock_Status = "Locked";
									if (!tt_writeLock.list_of_items_locked.contains(data_item)) {
										tt_writeLock.list_of_items_locked.add(data_item);
										transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);
									}
									System.out.println(transactionID + " acquires write lock on " + data_item);
								}

							}

						}
					} else if (lock1.lock_state.equals("write-locked")) {
						String Lock_tid = tID_holding_lock.get(0);
						holding_Trans_TS = transactionTablehashmap.get(Lock_tid).timeStamp;
						if (requesting_Trans_TS < holding_Trans_TS) {
							abort_transaction(tt_writeLock);
							lock1.lock_state = "write-locked";
							lock1.transactionID_holding_lock.add(transactionID);
							lock1.lock_Status = "Locked";
							if (!tt_writeLock.list_of_items_locked.contains(data_item)) {
								tt_writeLock.list_of_items_locked.add(data_item);
								transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);
							}
							System.out.println(transactionID + " acquires write lock on " + data_item);

						} else {
							String waiting_operation = "w" + transactionID.charAt(1) + "(" + data_item + ")";
							list_of_operation_waiting.add(waiting_operation);
							tt_writeLock.transaction_State = "Blocked";
							lock1.transactionID_waiting_for_lock.add(transactionID);
							transactionTablehashmap.put(tt_writeLock.transaction_ID, tt_writeLock);
							System.out.println(transactionID + " "
									+ "has been blocked as per wound-wait protocol for the data item " + " " + data_item
									+ " based on higher timestamp");
						}

					}
				}
			}

			// To check if the transaction is blocked
			else if (tt_writeLock.transaction_State.equals("Blocked")) {
				if (locktablehashmap.containsKey(data_item)) {
					String list_of_waiting_operation = "w" + transactionID.charAt(1) + "(" + data_item + ")";
					list_of_operation_waiting.add(list_of_waiting_operation);
					LockTable lock = locktablehashmap.get(data_item);
					lock.transactionID_waiting_for_lock.add(transactionID);
					locktablehashmap.put(data_item, lock);
					System.out.println(transactionID + " " + " is in blocked state and operation" + " "
							+ list_of_waiting_operation + " " + "is added to the list of operation waiting queue");

				}
				if (!tt_writeLock.list_of_items_locked.contains(data_item)) {
					tt_writeLock.list_of_items_locked.add(data_item);

				}

			}
			// To check if the transaction is aborted
			else if (tt_writeLock.transaction_State.equals("Aborted")) {
				System.out.println(transactionID + " " + "is already aborted");

			}

		}

	}

	// Method to implement end of a transaction
	public static void end_transaction(String trans) {
		Transaction_Table tTable = transactionTablehashmap.get(trans);
		// If transaction is active, call the commit_transaction method
		if (tTable.transaction_State == "Active") {
			commit_transaction(tTable);
		}
		// If the transaction is blocked, add the operation to the list of waiting
		// operation
		else if (tTable.transaction_State == "Blocked") {
			String endString = "e" + trans.charAt(1);
			list_of_operation_waiting.add(endString);
			System.out.println(tTable.transaction_ID + " is already blocked");
		}
		// If the Transaction is aborted
		else if (tTable.transaction_State == "Aborted") {
			System.out.println(tTable.transaction_ID + " " + "is already aborted");
		}
	}

	// Method to implement aborting a transaction
	public static void abort_transaction(Transaction_Table abort_trans) {

		String tID = abort_trans.transaction_ID;
		abort_trans.transaction_State = "Aborted";
		abort_trans.list_of_items_locked.clear();
		boolean item_removed_from_list = false;
		int aSize = list_of_operation_waiting.size();
		for (int k = 0; k < aSize;) {
			String operation_to_process = list_of_operation_waiting.get(0);
			if (tID.charAt(1) == operation_to_process.charAt(1)) {
				list_of_operation_waiting.remove(operation_to_process);
				aSize = aSize - 1;
				item_removed_from_list = true;
			}
			if (!(item_removed_from_list)) {
				break;
			}
		}
		releaseLock(tID);
		process_waiting_operations();

	}

	// Method to implement committing of a transaction
	public static void commit_transaction(Transaction_Table commit_trans) {

		commit_trans.transaction_State = "Committed";
		commit_trans.list_of_items_locked.clear();
		String tID = commit_trans.transaction_ID;
		System.out.println(tID + " is committed");
		releaseLock(tID);
		process_waiting_operations();

	}

	// Method to implement releasing all the locks held by the transaction
	public static void releaseLock(String tID) {

		for (String key : locktablehashmap.keySet()) {

			LockTable lock1 = locktablehashmap.get(key);
			if (lock1.transactionID_holding_lock.contains(tID)) {
				if (lock1.transactionID_holding_lock.size() == 1) {
					lock1.transactionID_holding_lock.remove(tID);
					if (lock1.lock_Status == "read-locked") {
						lock1.no_of_reads = lock1.no_of_reads - 1;
					}
					lock1.lock_Status = "Unlocked";
					lock1.lock_state = " ";
					data_item_to_process.add(lock1.item_name);
				} else if (lock1.transactionID_holding_lock.size() > 1) {
					lock1.transactionID_holding_lock.remove(tID);
					if (lock1.lock_Status == "read-locked") {
						lock1.no_of_reads = lock1.no_of_reads - 1;
					}
					data_item_to_process.add(lock1.item_name);
				}

			}
			if (lock1.transactionID_waiting_for_lock.contains(tID)) {
				lock1.transactionID_waiting_for_lock.remove(tID);
			}
		}

	}

	// Method to implement the processing of waiting operations
	public static void process_waiting_operations() {

		boolean transactionContinueToProcess = false;
		boolean noneProcessed = false;

		ArrayList<String> list_transactionContinueToWait = new ArrayList<String>();
		ArrayList<String> list_transactionContinueToProcess = new ArrayList<String>();
		int arraySize = list_of_operation_waiting.size();
		for (int j = 0; j < arraySize;) {

			String operation_to_process = list_of_operation_waiting.get(j);
			String transid = operation_to_process.substring(1, 2);
			String transOfOpToProcess = "T" + transid;
			Transaction_Table transTable = transactionTablehashmap.get(transOfOpToProcess);
			boolean exitLoop = false;
			if (!(list_transactionContinueToWait.contains(transid))) {
				for (int k = 0; k < data_item_to_process.size(); k++) {

					String dataItem_to_check = data_item_to_process.get(k);
					char ditem = dataItem_to_check.charAt(0);
					LockTable lockOfItemToProcess = locktablehashmap.get(dataItem_to_check);
					if (ditem == operation_to_process.charAt(3)) {
						if ((!exitLoop)) {
							transTable.transaction_State = "Active";
							transactionTablehashmap.put(transTable.transaction_ID, transTable);
							if (operation_to_process.charAt(0) == 'w') {
								write_lock("T" + operation_to_process.substring(1, 2), dataItem_to_check);
							} else if (operation_to_process.charAt(0) == 'r') {
								read_lock("T" + operation_to_process.substring(1, 2), dataItem_to_check);
							} else if (operation_to_process.charAt(0) == 'e') {
								end_transaction("T" + operation_to_process.substring(1, 2));
							}
							list_of_operation_waiting.remove(operation_to_process);
							lockOfItemToProcess.transactionID_waiting_for_lock.remove(transOfOpToProcess);
							transactionContinueToProcess = true;
							list_transactionContinueToProcess.add(transid);
							noneProcessed = true;
							arraySize = arraySize - 1;
							exitLoop = true;
						}
					}

				}
				if (exitLoop == false) {

					Iterator<String> trans_to_continue_process = list_transactionContinueToProcess.iterator();
					while (trans_to_continue_process.hasNext()) {
						String transInProcess = trans_to_continue_process.next();
						String data_item = operation_to_process.substring(operation_to_process.indexOf('(') + 1,
								operation_to_process.indexOf(')'));
						if (transInProcess.equals(transid)) {
							transTable.transaction_State = "Active";
							transactionTablehashmap.put(transTable.transaction_ID, transTable);
							if (operation_to_process.charAt(0) == 'w') {
								write_lock("T" + operation_to_process.substring(1, 2), data_item);
							} else if (operation_to_process.charAt(0) == 'r') {
								read_lock("T" + operation_to_process.substring(1, 2), data_item);
							} else if (operation_to_process.charAt(0) == 'e') {
								end_transaction("T" + operation_to_process.substring(1, 2));
							}
							list_of_operation_waiting.remove(operation_to_process);
							transactionContinueToProcess = true;
							list_transactionContinueToProcess.add(transid);
							arraySize = arraySize - 1;
							noneProcessed = true;
							break;

						}
					}

				}

				if (!transactionContinueToProcess) {
					list_transactionContinueToWait.add(transid);
				}
			}
			if (!(noneProcessed)) {
				break;
			}
		}

		data_item_to_process.clear();
	}

	// Main Method
	public static void main(String args[]) throws FileNotFoundException, IOException, StringIndexOutOfBoundsException {
		Rigorous_2PL obj = new Rigorous_2PL();
		obj.readFile();

	}

}
