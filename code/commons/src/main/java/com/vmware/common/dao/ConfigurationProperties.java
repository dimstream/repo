package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing configuration_properties DB table
 * configuration_properties - Stores the key,value pairs for every input  output connection 
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "configuration_properties")
public class ConfigurationProperties {
	
	@DatabaseField(generatedId = true)
    public int configurationId;
    
    @DatabaseField(canBeNull = false)
    public String configurationKey;
    
    @DatabaseField(canBeNull = false, columnDefinition="VARCHAR(3000)")
    public String configurationValue;
    
    @DatabaseField(foreign = true, canBeNull = false)
    public transient ConfigurationDetails configurationDetails;
}