package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing action DB table.
 * action - stores the action configurations against each trap
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "action")
public class Action {
	
	@DatabaseField(generatedId = true)
    public int actionId;
	
    @DatabaseField
    public String actionType;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient Trap trap;
    
    @ForeignCollectionField(eager = false)
    public ForeignCollection<TrapActionConfig> configuration;
    
}