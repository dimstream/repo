package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing process_context DB table
 * process_context - Stores the context information
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "process_context")
public class ProcessContext {
	
    @DatabaseField(id = true)
    public String processContextName;
    
    @DatabaseField(canBeNull = true)
    public String enrichmentCompletion;
    
    @DatabaseField(canBeNull = true)
    public Integer evictionTime; 
    
    @DatabaseField(canBeNull = true, defaultValue="false")
    public Boolean singleStream;
}
