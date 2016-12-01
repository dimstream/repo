package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing computed_states DB table
 * computed_states - Stores the fields to be evaluated, computed and stored in the context.
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "computed_states")
public class ComputedStates {
	
    @DatabaseField(generatedId = true)
    public Integer computedStatesId;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient ContextMapping contextMapping;
    
    @DatabaseField(canBeNull = false)
    public String stateName;
    
    @DatabaseField(canBeNull = false)
    public String conditions;
}