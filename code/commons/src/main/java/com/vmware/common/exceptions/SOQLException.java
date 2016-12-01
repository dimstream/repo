package com.vmware.common.exceptions;

/**
 * User defined Exception class for SOQL<br>
 * Thrown if the maximum number of tries to login is finished
 * 
 * @author ghimanshu
 *
 */
public class SOQLException extends Exception {

	private static final long serialVersionUID = 1L;

	public SOQLException(String msg) {
		super(msg);
	}

	public SOQLException(String msg, Throwable t) {
		super(msg, t);
	}
}
