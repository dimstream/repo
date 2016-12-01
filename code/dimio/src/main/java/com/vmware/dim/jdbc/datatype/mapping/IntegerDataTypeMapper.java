package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;

public class IntegerDataTypeMapper {

	public static Integer map(Object value, String conf, Connection connection){
		return Integer.valueOf(value.toString());
	}

}
