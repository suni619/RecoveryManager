package com.suni619.uta.recoverymanager.entity;

public class Checkpoint {
	Integer checkpointIndex;
	Integer checkpointOffset;
	
	public Checkpoint() {
	}

	public Integer getCheckpointIndex() {
		return checkpointIndex;
	}

	public void setCheckpointIndex(Integer checkpointIndex) {
		this.checkpointIndex = checkpointIndex;
	}

	public Integer getCheckpointOffset() {
		return checkpointOffset;
	}

	public void setCheckpointOffset(Integer checkpointOffset) {
		this.checkpointOffset = checkpointOffset;
	}

	@Override
	public String toString() {
		return "Checkpoint [checkpointIndex=" + checkpointIndex
				+ ", checkpointOffset=" + checkpointOffset + "]";
	}
	
}
