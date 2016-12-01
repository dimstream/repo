package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;;

/**
 * Entity class representing configuration_details DB table
 * configuration_details - Stores the input and output connection details
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "configuration_details")
public class ConfigurationDetails {
    
    @DatabaseField(id = true)
    public String configurationName;
    
    @DatabaseField(canBeNull = false)
    public String configurationType;
    
    @DatabaseField(canBeNull = true)
	public String configurationFrequency;
    
    @DatabaseField(canBeNull = false)
	public String configurationFlow;
    
    @DatabaseField(canBeNull = true)
	public String streamName;
    
    @ForeignCollectionField(eager = false)
    public ForeignCollection<ConfigurationProperties> configurationProperties;
}