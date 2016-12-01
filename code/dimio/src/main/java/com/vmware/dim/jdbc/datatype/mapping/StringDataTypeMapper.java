package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;

public class StringDataTypeMapper {

	public static String map(Object value, String conf, Connection connection){
		return value.toString();
	}

}
