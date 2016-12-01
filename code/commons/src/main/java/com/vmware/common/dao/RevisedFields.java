package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing persisted_fields DB table
 * revised_fields - Stores the revised fields to be persisted in the context.
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "revised_fields")
public class RevisedFields {
	
    @DatabaseField(generatedId = true)
    public Integer revisedFieldsId;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient ContextMapping contextMapping;
    
    @DatabaseField(canBeNull = false)
    public String fieldName;
    
    @DatabaseField(canBeNull = false)
    public String expression;
}