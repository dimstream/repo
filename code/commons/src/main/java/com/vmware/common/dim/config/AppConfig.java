package com.vmware.common.dim.config;

import java.io.Serializable;

/**
 * Configuration class to hold the configuration data provided to the application as a POJO. 
 * 
 * @author vedanthr
 *
 */
public class AppConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	public String AppName = "Real time processing engine";
    public int batchSize = 5;
    public String stopSignal = "CSig:STOP";
    public String checkpoint;
    public KafkaConfig kafka;
    public FrameworkManagerConfig frameworkManager;
    public CacheEngineConfig cacheEngine;
}
