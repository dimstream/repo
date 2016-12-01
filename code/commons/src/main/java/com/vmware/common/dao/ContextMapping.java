package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing context_mapping DB table
 * context_mapping - Stores the mapping definitions against each stream
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "context_mapping")
public class ContextMapping {
	
    @DatabaseField(generatedId = true)
    public Integer contextMappingId;
    
    @DatabaseField(canBeNull = false, foreign = true, uniqueCombo = true, columnName="Stream_id")
    public EventStream stream;
    
    @DatabaseField(canBeNull = false, foreign = true, uniqueCombo = true, columnName="Context_id")
    public ProcessContext context;
    
    @DatabaseField(canBeNull = false)
    public Boolean primaryStream;
    
    @DatabaseField(canBeNull = true)
    public String joinConditions;
    
    @DatabaseField(canBeNull = true)
    public String nestedField;
    
    @ForeignCollectionField(eager = true)
    public ForeignCollection<PersistedFields> persistedFields;
    
    @ForeignCollectionField(eager = true)
    public ForeignCollection<ComputedStates> computedStates;
    
    @DatabaseField(canBeNull = true, foreign = true, foreignColumnName="externalLookUpId" )
    public ExternalLookUp externalLookUp;

    @ForeignCollectionField(eager = true)
    public ForeignCollection<RevisedFields> revisedFields;
}