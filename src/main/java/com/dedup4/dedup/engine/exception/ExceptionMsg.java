package com.dedup4.dedup.engine.exception;

public enum ExceptionMsg {
	EXCEPTION001("ChunkId in MongoDB is not unique.");
	
	private String exceptionMsg;
	
	private ExceptionMsg(String exceptionMsg) {
		this.exceptionMsg = exceptionMsg;
	}
	
	public String getExceptionMsg() {
		return this.exceptionMsg;
	}
}
