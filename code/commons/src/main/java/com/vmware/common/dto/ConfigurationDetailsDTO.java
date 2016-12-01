package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming ConfigurationDetails json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ConfigurationDetailsDTO {
	public String configurationName;
	public String configurationType;
	public String configurationFrequency;
	public String configurationFlow;
	public String streamName;
	public List<ConfigurationPropertiesDTO> configurationProperties;
}