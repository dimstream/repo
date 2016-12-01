package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;

public class FloatDataTypeMapper {

	public static Float map(Object value, String conf, Connection connection){
		return Float.valueOf(value.toString());
	}

}
