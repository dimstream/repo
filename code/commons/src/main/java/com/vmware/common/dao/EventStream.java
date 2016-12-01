package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing event_stream DB table
 * event_stream - Stores the event stream definition, the pk identifier and filter conditions. 
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "event_stream")
public class EventStream {
	
    @DatabaseField(id = true)
    public String eventStreamName;
    
    @ForeignCollectionField(eager = false)
    public ForeignCollection<Identifier> identifier;
    
    @DatabaseField
    public String filter;
    
    @DatabaseField(canBeNull = true)
    public Integer evictionTime; 
    
    @DatabaseField(canBeNull = true, columnDefinition="VARCHAR(3000)")
    public String lateJoinCondition;
}