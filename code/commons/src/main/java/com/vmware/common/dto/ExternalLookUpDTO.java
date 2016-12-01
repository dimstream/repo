package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming ExternalLookUp json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ExternalLookUpDTO {
    public String loginURL;
    public String methodType;
    public String authType;
    public String contentTypeHeader;
    public String tokenKey;
    public List<URLTagDTO> urlTag;
}