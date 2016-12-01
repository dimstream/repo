package com.vmware.common.dto;

import java.util.ArrayList;

/**
 * DTO class to transform incoming StreamConfiguration json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class StreamConfigurationDTO {
    // entity definition class to absorb json payloads of configuration
    public EventStreamDTO stream;
    public ArrayList<ContextMappingOutputDTO> contextMappings;
}