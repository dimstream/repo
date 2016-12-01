package com.vmware.common.dim.config;

import java.io.Serializable;

/**
 * Configuration class to hold the Kafka configuration data provided to the application as a POJO. 
 * 
 * @author vedanthr
 *
 */
public class KafkaConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	public String streamSourceBrokers;
    public String streamSourceTopics;
}
