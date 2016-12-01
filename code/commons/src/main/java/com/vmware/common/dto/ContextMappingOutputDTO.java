package com.vmware.common.dto;

import java.util.ArrayList;

/**
 * DTO class to transform incoming ContextMappingOutput json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ContextMappingOutputDTO {
    public ProcessContextDTO context;
    public boolean primaryStream;
    public String joinConditions;
    public String nestedField;
    public ArrayList<PersistedFieldDTO> persistedFields;
    public ArrayList<ComputedStateDTO> computedStates;
    public ArrayList<RevisedFieldDTO> revisedFields;
    public ExternalLookUpDTO externalLookUp;
}