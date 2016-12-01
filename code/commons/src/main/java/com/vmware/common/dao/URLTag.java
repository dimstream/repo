package com.vmware.common.dao;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * Entity class representing url_tag DB table
 * url_tag - Stores the external lookup connection details 
 * 
 * @author vedanthr
 */
@DatabaseTable(tableName = "url_tag")
public class URLTag {
	
    @DatabaseField(generatedId = true)
    public Integer urlTagId;
    
    @DatabaseField(canBeNull = false, foreign = true)
    public transient ExternalLookUp externalLookUp;
    
    @DatabaseField(canBeNull = false, columnDefinition="VARCHAR(3000)")
    public String url;
    
    @DatabaseField(canBeNull = false)
    public String methodType;
    
    @DatabaseField(canBeNull = false)
    public String contentTypeHeader;
    
    @DatabaseField(canBeNull = false)
    public String authorizationHeader;
    
    @DatabaseField(canBeNull = false)
    public String tag;
    
    @DatabaseField(canBeNull = false)
    public String queryParam;
    
}