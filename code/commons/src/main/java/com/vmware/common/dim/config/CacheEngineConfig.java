package com.vmware.common.dim.config;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration class to hold the Redis configuration data provided to the application as a POJO. 
 * 
 * @author vedanthr
 *
 */
public class CacheEngineConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	public List<String> host;
    public List<Integer> port;
    public String globalConfigKey;
    public String globalConfigMember;
}
