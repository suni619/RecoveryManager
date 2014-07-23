package com.suni619.uta.recoverymanager.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.suni619.uta.recoverymanager.entity.CacheTableEntry;
import com.suni619.uta.recoverymanager.entity.Checkpoint;
import com.suni619.uta.recoverymanager.entity.Log;
import com.suni619.uta.recoverymanager.entity.Operation;
import com.suni619.uta.recoverymanager.entity.Status;
import com.suni619.uta.recoverymanager.entity.Transaction;

public class RecoveryManager {

	private static final int LOG_BUFFER_SIZE = 4;
	private static Map<Character, Integer> diskBlockInMemory;
	private static Map<Character, Integer> diskBlockInCache;
	private static List<CacheTableEntry> cacheTable;
	private static List<Integer> emptyBufferList;
	private static List<Log> logBuffer;
	private static List<List<Log>> logFile;
	private static List<Transaction> transactionTable;
	private static Checkpoint previousCheckpoint;
	private static StringBuffer output = new StringBuffer();

	public static void main(String[] args) {

		// initialize all the variables
		initializeDBMS();
		// read the input from file
		String input = readInput("input2.txt");
		if (input != null) {
			// get the operations from input
			String[] operations = getOperations(input);
			// process operations
			processOperations(operations);

			writeToOutput("Success.");
		} else {
			writeToOutput("Input error");
		}
		writeToOutput("Final values on disk: " + diskBlockInMemory);
		writeOutputFile("output2.txt", output.toString());
		System.out.println("Done");

	}

	public static void initializeDBMS() {
		diskBlockInMemory = new HashMap<Character, Integer>();
		diskBlockInCache = new HashMap<Character, Integer>();
		cacheTable = new ArrayList<CacheTableEntry>();
		emptyBufferList = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4,
				5, 6, 7, 8, 9));
		logBuffer = new ArrayList<Log>();
		logFile = new ArrayList<List<Log>>();
		transactionTable = new ArrayList<Transaction>();
		previousCheckpoint = new Checkpoint();
		previousCheckpoint.setCheckpointIndex(0);
		previousCheckpoint.setCheckpointOffset(0);

	}

	private static void writeToOutput(String msg) {
		System.out.println(msg);
		output.append(msg + "\n");
	}

	private static void writeOutputFile(String filename, String output) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			writer.write(output);
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void processOperations(String[] operations) {
		for (String operation : operations) {
			// process each operation
			switch (operation.charAt(0)) {
			case 'b': // do begin operation
				begin(operation);
				break;
			case 'r': // do read operation
				read(operation);
				break;
			case 'w': // do write operation
				write(operation);
				break;
			case 'c': // do commit operation
				commit(operation);
				break;
			case 'C': // do checkpoint operation
				checkpoint(operation);
				break;
			case 'F': // do Failure operation
				failure(operation);
				break;
			}
		}
	}

	private static void begin(String operation) {
		writeToOutput(operation);

		// get transaction id from the operation
		Integer transactionId = Character.getNumericValue(getParameter(
				"transaction id", operation));

		// write to log buffer
		Log log = new Log();
		log.setTransactionId(transactionId);
		log.setOperation(Operation.BEGIN);
		logBuffer.add(log);

		// if log buffer is full
		if (logBuffer.size() == LOG_BUFFER_SIZE) {
			// create a clone of log buffer
			List<Log> tempLogBuffer = new ArrayList<Log>();
			for (Log tempLog : logBuffer) {
				tempLogBuffer.add(tempLog);
			}
			// write buffer to disk
			logFile.add(tempLogBuffer);
			// empty buffer
			logBuffer.clear();
		}

		// add to transaction table
		Transaction transaction = new Transaction();
		transaction.setTransactionId(transactionId);
		transaction.setTimestamp(new Date());
		transaction.setStatus(Status.ACTIVE);
		transaction.setItemsWritten(new ArrayList<Character>());
		transactionTable.add(transaction);

		writeToOutput("Transaction table:" + transactionTable);
	}

	private static Character getParameter(String param, String operation) {
		Character parameter = null;
		if (param.equals("transaction id")) {
			parameter = operation.charAt(1);
		} else if (param.equals("item")) {
			parameter = operation.charAt(3);
		} else if (param.equals("bfim")) {
			parameter = operation.charAt(5);
		} else if (param.equals("afim")) {
			parameter = operation.charAt(7);
		}
		return parameter;
	}

	private static void read(String operation) {
		writeToOutput(operation);

		// get transaction id from the operation
		Integer transactionId = Character.getNumericValue(getParameter(
				"transaction id", operation));
		Character item = getParameter("item", operation);

		// write to log buffer
		Log log = new Log();
		log.setTransactionId(transactionId);
		log.setOperation(Operation.READ);
		log.setItem(item);
		logBuffer.add(log);

		// if log buffer is full
		if (logBuffer.size() == LOG_BUFFER_SIZE) {
			// create a clone of log buffer
			List<Log> tempLogBuffer = new ArrayList<Log>();
			for (Log tempLog : logBuffer) {
				tempLogBuffer.add(tempLog);
			}
			// write buffer to disk
			logFile.add(tempLogBuffer);
			// empty buffer
			logBuffer.clear();
		}

		// check item in cache table
		boolean isCached = checkCacheTableForItem(item);

		// if not found
		if (!isCached) {
			// take from disk
			Integer value = getItemFromDisk(item);
			// insert to disk block in cache
			diskBlockInCache.put(item, value);
			// add to cache table
			CacheTableEntry cacheTableEntry = new CacheTableEntry();
			// take block number from empty buffer list
			cacheTableEntry.setBlockNumber(allocateCacheBlock());
			cacheTableEntry.setItem(item);
			cacheTableEntry.setDirtyBit(false);
			cacheTableEntry.setPinBit(false);
			List<Integer> transactionIds = new ArrayList<Integer>();
			transactionIds.add(transactionId);
			cacheTableEntry.setTransactionIds(transactionIds);
			cacheTable.add(cacheTableEntry);
		}
		// if found
		else {
			// update cache table for transaction id list
			updateCacheTable(item, transactionId, false);
		}
		// read item
		readItem(item);
	}

	private static Integer allocateCacheBlock() {
		Integer i = emptyBufferList.remove(0);
		writeToOutput("Allocated cache block: " + i);
		return i;
	}

	private static void updateCacheTable(Character item, Integer transactionId,
			boolean write) {
		for (CacheTableEntry entry : cacheTable) {
			if (entry.getItem() == item) {
				List<Integer> transactionIds = entry.getTransactionIds();
				// add if the transaction is not present already
				if (!transactionIds.contains(transactionId)) {
					transactionIds.add(transactionId);
					entry.setTransactionIds(transactionIds);
				}
				if (write) {
					entry.setDirtyBit(true);
				}
				break;
			}
		}
	}

	private static void readItem(Character item) {
		diskBlockInCache.get(item);
	}

	private static Integer getItemFromDisk(Character item) {
		return diskBlockInMemory.get(item);
	}

	private static boolean checkCacheTableForItem(Character item) {
		boolean isCached = false;
		for (CacheTableEntry entry : cacheTable) {
			if (entry.getItem().equals(item)) {
				isCached = true;
				break;
			}
		}
		return isCached;
	}

	private static void write(String operation) {
		writeToOutput(operation);

		// get transaction id from the operation
		Integer transactionId = Character.getNumericValue(getParameter(
				"transaction id", operation));
		Character item = getParameter("item", operation);
		Integer bfim = Character
				.getNumericValue(getParameter("bfim", operation));
		Integer afim = Character
				.getNumericValue(getParameter("afim", operation));

		// write to log buffer
		Log log = new Log();
		log.setTransactionId(transactionId);
		log.setOperation(Operation.WRITE);
		log.setItem(item);
		log.setBfim(bfim);
		log.setAfim(afim);
		logBuffer.add(log);

		// if log buffer is full
		if (logBuffer.size() == LOG_BUFFER_SIZE) {
			// create a clone of log buffer
			List<Log> tempLogBuffer = new ArrayList<Log>();
			for (Log tempLog : logBuffer) {
				tempLogBuffer.add(tempLog);
			}
			// write buffer to disk
			logFile.add(tempLogBuffer);
			// empty buffer
			logBuffer.clear();
		}

		// check item in cache table
		boolean isCached = checkCacheTableForItem(item);

		// if not found
		if (!isCached) {
			// take from disk
			Integer value = getItemFromDisk(item);
			// insert value to disk block in cache
			diskBlockInCache.put(item, value);
			// add to cache table
			CacheTableEntry cacheTableEntry = new CacheTableEntry();
			// take block number from empty buffer list
			cacheTableEntry.setBlockNumber(allocateCacheBlock());
			cacheTableEntry.setItem(item);
			cacheTableEntry.setDirtyBit(true);
			cacheTableEntry.setPinBit(false);
			List<Integer> transactionIds = new ArrayList<Integer>();
			transactionIds.add(transactionId);
			cacheTableEntry.setTransactionIds(transactionIds);
			cacheTable.add(cacheTableEntry);

			// update transaction table for list of items written
			updateTransactionTable(transactionId, item, false);
		}
		// if found
		else {
			// update cache table for transaction id list
			// update cache table for dirty bit
			updateCacheTable(item, transactionId, true);

			// update transaction table for list of items written
			updateTransactionTable(transactionId, item, false);

		}
		// write item
		writeItem(item, afim);

		writeToOutput(transactionTable.toString());
	}

	private static void writeItem(Character item, Integer afim) {
		diskBlockInCache.put(item, afim);
	}

	private static void updateTransactionTable(Integer transactionId,
			Character item, boolean commit) {
		for (Transaction transaction : transactionTable) {
			if (transaction.getTransactionId() == transactionId) {
				if (commit) {
					transaction.setStatus(Status.COMMITTED);
				} else {
					List<Character> itemsWritten = transaction
							.getItemsWritten();
					// add if the item is not present already
					if (!itemsWritten.contains(item)) {
						itemsWritten.add(item);
						transaction.setItemsWritten(itemsWritten);
					}
				}
				break;
			}
		}
	}

	private static void commit(String operation) {
		writeToOutput(operation);

		// get transaction id from the operation
		Integer transactionId = Character.getNumericValue(getParameter(
				"transaction id", operation));

		// write to log buffer
		Log log = new Log();
		log.setTransactionId(transactionId);
		log.setOperation(Operation.COMMIT);
		logBuffer.add(log);

		// flush log buffer to disk
		// create a clone of log buffer
		List<Log> tempLogBuffer = new ArrayList<Log>();
		for (Log tempLog : logBuffer) {
			tempLogBuffer.add(tempLog);
		}
		// write buffer to disk
		logFile.add(tempLogBuffer);
		logBuffer.clear();

		// update transaction table for status
		updateTransactionTable(transactionId, null, true);

		// commit transaction
		commit();
	}

	private static void commit() {
	}

	private static void checkpoint(String operation) {
		writeToOutput(operation);

		// flush dirty disk blocks to memory
		flushDirtyBlocksToMemory();

		// store transaction table to log
		// write to log buffer
		Log log = new Log();
		log.setOperation(Operation.CHECKPOINT);
		// create a clone of transaction table
		List<Transaction> tempTransactionTable = new ArrayList<Transaction>();
		for (Transaction transaction : transactionTable) {
			Transaction tempTransaction = new Transaction();
			tempTransaction.setTransactionId(transaction.getTransactionId());
			tempTransaction.setTimestamp(transaction.getTimestamp());
			tempTransaction.setStatus(transaction.getStatus());
			List<Character> itemsWritten = transaction.getItemsWritten();
			List<Character> tempItemsWritten = new ArrayList<Character>();
			for (Character itemWritten : itemsWritten) {
				tempItemsWritten.add(itemWritten);
			}
			tempTransaction.setItemsWritten(tempItemsWritten);

			tempTransactionTable.add(tempTransaction);
		}
		log.setTransactionTable(tempTransactionTable);
		logBuffer.add(log);

		// update checkpoint offset
		previousCheckpoint.setCheckpointOffset(logBuffer.size() - 1);

		// flush log buffer to disk
		// create a clone of log buffer
		List<Log> tempLogBuffer = new ArrayList<Log>();
		for (Log tempLog : logBuffer) {
			tempLogBuffer.add(tempLog);
		}
		// write buffer to disk
		logFile.add(tempLogBuffer);
		logBuffer.clear();

		// update checkpoint index
		previousCheckpoint.setCheckpointIndex(logFile.size() - 1);

		writeToOutput("Log file: " + logFile);

	}

	private static void flushDirtyBlocksToMemory() {
		for (CacheTableEntry entry : cacheTable) {
			if (entry.isDirtyBit()) {
				diskBlockInMemory.put(entry.getItem(),
						diskBlockInCache.get(entry.getItem()));
				// reset the dirty bit
				entry.setDirtyBit(false);
			}
		}
	}

	private static void failure(String operation) {
		writeToOutput(operation);
		writeToOutput("Failure occured");
		writeToOutput("Recovery in progress...");
		writeToOutput("Log file: " + logFile);

		// open the log
		// find previous checkpoint and return transaction table at the
		// checkpoint
		List<Transaction> transactionTableAtCheckpoint = getTransactionTableAtCheckpoint();

		writeToOutput("Transaction table at checkpoint"
				+ transactionTableAtCheckpoint);

		// get list of committed transactions after the checkpoint
		List<Integer> committedTransactionsAfterCheckpoint = getCommittedTransactionsAfterCheckpoint(transactionTableAtCheckpoint);

		writeToOutput("Committed transactions after checkpoint: "
				+ committedTransactionsAfterCheckpoint);
		// find active transactions
		// for each transaction
		for (Transaction transaction : transactionTableAtCheckpoint) {
			if (transaction.getStatus().equals(Status.ACTIVE)) {
				// check if it is committed after the checkpoint
				// if committed
				if (committedTransactionsAfterCheckpoint.contains(transaction
						.getTransactionId())) {
					// continue from checkpoint and redo all operations of that
					// transaction
					// till commit
					redo(transaction.getTransactionId());
				}
				// if not committed
				else {
					// rollback the transaction by undoing till the begin
					rollback(transaction.getTransactionId());
				}
			}
		}

		// find transactions at checkpoint
		List<Integer> transactionsAtCheckpoint = getTransactionsAtCheckpoint();
		// redo new committed transactions that begun after the checkpoint
		for (Integer transactionId : committedTransactionsAfterCheckpoint) {
			if (!transactionsAtCheckpoint.contains(transactionId)) {
				redo(transactionId);
			}
		}
	}

	private static List<Integer> getTransactionsAtCheckpoint() {
		List<Transaction> transactionTableAtCheckpoint = getTransactionTableAtCheckpoint();
		List<Integer> transactionsAtCheckpoint = new ArrayList<Integer>();
		for (Transaction transaction : transactionTableAtCheckpoint) {
			transactionsAtCheckpoint.add(transaction.getTransactionId());
		}
		return transactionsAtCheckpoint;
	}

	private static void rollback(Integer transactionId) {
		writeToOutput("Transaction " + transactionId + " needs to be undone.");

		// till the beginning of the transaction
		for (int i = previousCheckpoint.getCheckpointIndex(); i >= 0; i--) {
			for (int j = ((i == previousCheckpoint.getCheckpointIndex()) ? previousCheckpoint
					.getCheckpointOffset() : (logFile.get(i).size() - 1)); j >= 0; j--) {
				// if transaction id is different or operation is read
				if (logFile.get(i).get(j).getTransactionId() != transactionId
						|| logFile.get(i).get(j).getOperation()
								.equals(Operation.READ)) {
					// skip

				}
				// if transaction id is same and operation is begin
				else if (logFile.get(i).get(j).getTransactionId() == transactionId
						&& logFile.get(i).get(j).getOperation()
								.equals(Operation.BEGIN)) {
					// return
					return;
				}
				// if transaction id is same and operation is write
				else if (logFile.get(i).get(j).getTransactionId() == transactionId
						|| logFile.get(i).get(j).getOperation()
								.equals(Operation.WRITE)) {
					// write before image to disk
					diskBlockInMemory.put(logFile.get(i).get(j).getItem(),
							logFile.get(i).get(j).getBfim());
					writeToOutput("Wrote before image: "
							+ logFile.get(i).get(j).getItem() + "="
							+ logFile.get(i).get(j).getBfim());
				}

			}
		}
	}

	private static void redo(Integer transactionId) {
		writeToOutput("Transaction " + transactionId + " needs to be redone.");
		for (int i = previousCheckpoint.getCheckpointIndex(); i < logFile
				.size(); i++) {
			for (int j = ((i == previousCheckpoint.getCheckpointIndex()) ? previousCheckpoint
					.getCheckpointOffset() : 0); j < logFile.get(i).size(); j++) {
				// if transaction id is different or operation is read
				if (logFile.get(i).get(j).getTransactionId() != transactionId
						|| logFile.get(i).get(j).getOperation()
								.equals(Operation.READ)
						|| logFile.get(i).get(j).getOperation()
								.equals(Operation.BEGIN)) {
					// skip

				}
				// if transaction id is same and operation is commit
				else if (logFile.get(i).get(j).getTransactionId() == transactionId
						&& logFile.get(i).get(j).getOperation()
								.equals(Operation.COMMIT)) {
					// return
					return;
				}
				// if transaction id is same and operation is write
				else if (logFile.get(i).get(j).getTransactionId() == transactionId
						|| logFile.get(i).get(j).getOperation()
								.equals(Operation.WRITE)) {
					// write after image to disk
					diskBlockInMemory.put(logFile.get(i).get(j).getItem(),
							logFile.get(i).get(j).getAfim());
					writeToOutput("Wrote after image: "
							+ logFile.get(i).get(j).getItem() + "="
							+ logFile.get(i).get(j).getAfim());
				}
			}
		}
	}

	private static List<Integer> getCommittedTransactionsAfterCheckpoint(
			List<Transaction> transactionTableAtCheckpoint) {
		List<Integer> committedTransactionsAfterCheckpoint = new ArrayList<Integer>();
		for (int i = previousCheckpoint.getCheckpointIndex(); i < logFile
				.size(); i++) {
			for (int j = ((i == previousCheckpoint.getCheckpointIndex()) ? previousCheckpoint
					.getCheckpointOffset() : 0); j < logFile.get(i).size(); j++) {
				if (logFile.get(i).get(j).getOperation()
						.equals(Operation.COMMIT)) {
					committedTransactionsAfterCheckpoint.add(logFile.get(i)
							.get(j).getTransactionId());
				}
			}
		}
		return committedTransactionsAfterCheckpoint;
	}

	private static List<Transaction> getTransactionTableAtCheckpoint() {
		List<Transaction> transactionTableAtCheckpoint = logFile
				.get(previousCheckpoint.getCheckpointIndex())
				.get(previousCheckpoint.getCheckpointOffset())
				.getTransactionTable();
		return transactionTableAtCheckpoint;
	}

	public static String readInput(String filename) {
		BufferedReader reader = null;
		String input = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			reader = new BufferedReader(new FileReader(filename));
			while ((input = reader.readLine()) != null) {
				stringBuilder.append(input);
			}
			input = stringBuilder.toString();
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return input;
	}

	private static String[] getOperations(String input) {
		// remove all white spaces and split the operations delimited by
		// semi-colon
		return input.replaceAll("[ ]", "").split("[;]");
	}

}
