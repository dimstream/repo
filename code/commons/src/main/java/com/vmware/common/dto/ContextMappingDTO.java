package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming ContextMapping json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ContextMappingDTO {
    
    public Integer contextMappingId;
    public EventStreamDTO stream;
    public ProcessContextDTO context;
    public Boolean primaryStream;
    public String joinConditions;
    public String nestedField;
    public List<PersistedFieldDTO> persistedFields;
    public List<ComputedStateDTO> computedStates;
    public List<RevisedFieldDTO> revisedFields;
    public ExternalLookUpDTO externalLookUp;
}