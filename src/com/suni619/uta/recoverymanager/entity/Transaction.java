package com.suni619.uta.recoverymanager.entity;
import java.util.Date;
import java.util.List;


public class Transaction {
	private Integer transactionId;
	private Date timestamp;
	private Status status;
	private List<Character> itemsWritten;
	
	public Transaction() {
	}

	public Integer getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(Integer transactionId) {
		this.transactionId = transactionId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public List<Character> getItemsWritten() {
		return itemsWritten;
	}

	public void setItemsWritten(List<Character> itemsWritten) {
		this.itemsWritten = itemsWritten;
	}

	@Override
	public String toString() {
		return "Transaction [transactionId=" + transactionId + ", timestamp="
				+ timestamp + ", status=" + status + ", itemsWritten="
				+ itemsWritten + "]";
	}

}
