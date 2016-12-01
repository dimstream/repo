package com.vmware.common.dto;

import java.util.ArrayList;

/**
 * DTO class to transform incoming GlobalConfiguration json string to a POJO
 * 
 * @author vedanthr
 *
 */
public class GlobalConfigurationDTO {
    public ArrayList<StreamConfigurationDTO> globalConfig;
}