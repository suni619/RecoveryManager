package com.suni619.uta.recoverymanager.entity;
import java.util.List;

public class Log {

	private Integer transactionId;
	private Operation operation;
	private Character item;
	private Integer bfim;
	private Integer afim;
	private List<Transaction> transactionTable;

	public Log() {
	}

	public Integer getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(Integer transactionId) {
		this.transactionId = transactionId;
	}

	public Operation getOperation() {
		return operation;
	}

	public void setOperation(Operation operation) {
		this.operation = operation;
	}

	public Character getItem() {
		return item;
	}

	public void setItem(Character item) {
		this.item = item;
	}

	public Integer getBfim() {
		return bfim;
	}

	public void setBfim(Integer bfim) {
		this.bfim = bfim;
	}

	public Integer getAfim() {
		return afim;
	}

	public void setAfim(Integer afim) {
		this.afim = afim;
	}

	public List<Transaction> getTransactionTable() {
		return transactionTable;
	}

	public void setTransactionTable(List<Transaction> transactionTable) {
		this.transactionTable = transactionTable;
	}

	@Override
	public String toString() {
		return "Log [transactionId=" + transactionId + ", operation="
				+ operation + ", item=" + item + ", bfim=" + bfim + ", afim="
				+ afim + ", transactionTable=" + transactionTable + "]";
	}

}
