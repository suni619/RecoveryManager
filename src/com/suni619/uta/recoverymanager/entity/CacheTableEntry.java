package com.suni619.uta.recoverymanager.entity;
import java.util.List;

public class CacheTableEntry {

	private int blockNumber;
	private Character item;
	private boolean dirtyBit;
	private boolean pinBit;
	private List<Integer> transactionIds;

	public CacheTableEntry() {
	}

	public int getBlockNumber() {
		return blockNumber;
	}

	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
	}

	public Character getItem() {
		return item;
	}

	public void setItem(Character item) {
		this.item = item;
	}

	public boolean isDirtyBit() {
		return dirtyBit;
	}

	public void setDirtyBit(boolean dirtyBit) {
		this.dirtyBit = dirtyBit;
	}

	public boolean isPinBit() {
		return pinBit;
	}

	public void setPinBit(boolean pinBit) {
		this.pinBit = pinBit;
	}

	public List<Integer> getTransactionIds() {
		return transactionIds;
	}

	public void setTransactionIds(List<Integer> transactionIds) {
		this.transactionIds = transactionIds;
	}

	@Override
	public String toString() {
		return "CacheTableEntry [blockNumber=" + blockNumber + ", item=" + item
				+ ", dirtyBit=" + dirtyBit + ", pinBit=" + pinBit
				+ ", transactionIds=" + transactionIds + "]";
	}

}
