package com.vmware.common.dim.config;

import java.io.Serializable;

/**
 * Configuration class to hold the manager configuration data provided to the application as a POJO. 
 * 
 * @author vedanthr
 *
 */
public class FrameworkManagerConfig implements Serializable {
	private static final long serialVersionUID = 1L;
	public String url;
    public int port;
    public String globalConfEndpoint;
    public String stopEndpoint;
}
