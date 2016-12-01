package com.vmware.common.action;

import com.vmware.common.dto.ActionDTO;

/**
 * Interface to be implemented by each action type
 * 
 * @author vedanthr
 */
public interface ActionInterface {
	public boolean doAction(ActionDTO actionDTO, String data);
}
