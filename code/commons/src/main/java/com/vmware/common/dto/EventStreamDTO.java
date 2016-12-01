package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming EventStream json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class EventStreamDTO {
    public String eventStreamName;
    public List<IdentifierDTO> identifier;
    public String filter;
    public Integer evictionTime;
    public String lateJoinCondition;
}