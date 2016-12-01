package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing persisted_fields DB table
 * persisted_fields - Stores the fields to be persisted in the context.
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "persisted_fields")
public class PersistedFields {
	
    @DatabaseField(generatedId = true)
    public Integer persistedFieldsId;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient ContextMapping contextMapping;
    
    @DatabaseField(canBeNull = false)
    public String streamFieldName;
    
    @DatabaseField(canBeNull = false)
    public String contextFieldName;
}