package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;

public class DoubleDataTypeMapper {

	public static Double map(Object value, String conf, Connection connection){
		return Double.valueOf(value.toString());
	}

}
