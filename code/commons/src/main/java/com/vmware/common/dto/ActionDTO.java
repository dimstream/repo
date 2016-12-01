package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming Action json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ActionDTO {
	public Integer actionId;
	public String actionType;
	public List<ConfigurationDTO> configuration;
}
