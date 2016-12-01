package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing trap DB table
 * trap - Stores the trap configuration for each connection 
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "trap")
public class Trap {
	
	@DatabaseField(generatedId = true)
    public Integer trapId;
	
    @DatabaseField
    public String condition;
    
    @DatabaseField
    public String joinCondition;
    
    @DatabaseField
    public String contextName;
    
    @DatabaseField
    public String pluginType;
    
    @DatabaseField
    public String pluginPoint;
    
    @ForeignCollectionField(eager = false)
    public ForeignCollection<Action> actions;
    
}