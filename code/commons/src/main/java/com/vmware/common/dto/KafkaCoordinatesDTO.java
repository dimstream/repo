package com.vmware.common.dto;

import java.io.Serializable;

public class KafkaCoordinatesDTO implements Serializable{
	private static final long serialVersionUID = 1L;
	public String metaDataBrokerList;
	public String serializerClass;
	public String keySerializerClass;
	public String topic;
}
