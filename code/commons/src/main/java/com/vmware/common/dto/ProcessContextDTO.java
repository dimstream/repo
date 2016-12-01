package com.vmware.common.dto;

/**
 * DTO class to transform incoming ProcessContext json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ProcessContextDTO {
    public String processContextName;
    public String enrichmentCompletion;
    public Integer evictionTime;
    public Boolean singleStream;
}