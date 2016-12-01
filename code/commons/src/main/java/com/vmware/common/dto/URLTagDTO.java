package com.vmware.common.dto;

/**
 * DTO class to transform incoming URLTag json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class URLTagDTO {
    
	public Integer urlTagId;
    public String url;
    public String methodType;
    public String contentTypeHeader;
    public String authorizationHeader;
    public String tag;
    public String queryParam;
}