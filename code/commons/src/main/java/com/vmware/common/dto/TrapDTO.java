package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming Trap json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class TrapDTO {
	public Integer trapId;
	public String condition;
	public String joinCondition;
	public String contextName;
	public String pluginType;
	public String pluginPoint;
    public List<ActionDTO> actions;
}
