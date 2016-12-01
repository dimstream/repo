package com.vmware.common.enums;

import com.vmware.common.action.PlayAction;
import com.vmware.common.action.impl.EmailNotificationAction;
import com.vmware.common.action.impl.IgnoreAction;

/**
 * Enum class to hold the action types.
 * 
 * @author vedanthr
 *
 */
public enum ActionEnum {

	email(EmailNotificationAction.class), 
	ignore(IgnoreAction.class);

	Class<? extends PlayAction> actionClass;

	private ActionEnum(Class<? extends PlayAction> actionClass) {
		this.actionClass = actionClass;
	}

	public Class<? extends PlayAction> getActionImplementationClass() {
		return this.actionClass;
	}
}
