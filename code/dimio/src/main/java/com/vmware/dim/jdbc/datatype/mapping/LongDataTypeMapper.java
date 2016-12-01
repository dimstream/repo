package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;

public class LongDataTypeMapper {

	public static Long map(Object value, String conf, Connection connection){
		return Long.valueOf(value.toString());
	}

}
