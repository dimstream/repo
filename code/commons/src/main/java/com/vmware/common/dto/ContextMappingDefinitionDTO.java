package com.vmware.common.dto;

import java.util.List;

/**
 * DTO class to transform incoming ContextMappingDefinition json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class ContextMappingDefinitionDTO {
    public EventStreamDTO stream;
    public List<ContextMappingDTO> contextMappings;
}