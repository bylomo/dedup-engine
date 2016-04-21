package com.dedup4.dedup.engine.exception;

public class DedupEngineException extends RuntimeException {
	private static final long serialVersionUID = -8057709538759476973L;
	
	public DedupEngineException(ExceptionMsg exceptionMsg){
		super(exceptionMsg.getExceptionMsg());
	}
}
