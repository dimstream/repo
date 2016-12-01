package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing trap_action_config DB table
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "trap_action_config")
public class TrapActionConfig {
	
	@DatabaseField(generatedId = true)
    public int configId;
	
	@DatabaseField(canBeNull = false, foreign = true)
    public transient Action action;
	
    @DatabaseField
    public String key;
    
    @DatabaseField
    public String value;
    
}