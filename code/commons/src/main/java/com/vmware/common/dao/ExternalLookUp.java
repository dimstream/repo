package com.vmware.common.dao;

import com.j256.ormlite.dao.ForeignCollection;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.ForeignCollectionField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing external_lookup DB table
 * external_lookup - Stores the external REST url details, which will be invoked while correlation
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "external_lookup")
public class ExternalLookUp {
	
    @DatabaseField(generatedId = true)
    public Integer externalLookUpId;
    
    @DatabaseField(canBeNull = true, columnDefinition="VARCHAR(3000)")
    public String loginURL;
    
    @DatabaseField(canBeNull = true)
    public String methodType;
    
    @DatabaseField(canBeNull = true)
    public String authType;
    
    @DatabaseField(canBeNull = true)
    public String contentTypeHeader;
    
    @DatabaseField(canBeNull = true)
    public String tokenKey;
    
    @ForeignCollectionField(eager = true, orderAscending = true, orderColumnName = "urlTagId")
    public ForeignCollection<URLTag> urlTag;
}