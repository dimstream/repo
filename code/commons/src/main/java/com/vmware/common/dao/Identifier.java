package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing identifier DB table
 * identifier - Stores the pk identifier for the stream 
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "identifier")
public class Identifier {
	
    @DatabaseField(generatedId = true)
    public int identifierId;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient EventStream eventStream;
    
    @DatabaseField(canBeNull = true)
    public String field;
    
}