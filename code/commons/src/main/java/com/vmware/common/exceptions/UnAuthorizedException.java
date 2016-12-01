package com.vmware.common.exceptions;

/**
 * User defined Exception class for SOQL<br>
 * Thrown if the session get expired or accessed in unauthorized manner
 * 
 * @author ghimanshu
 *
 */
public class UnAuthorizedException extends Exception {

	private static final long serialVersionUID = 1L;

	public UnAuthorizedException(String msg) {
		super(msg);
	}
}
