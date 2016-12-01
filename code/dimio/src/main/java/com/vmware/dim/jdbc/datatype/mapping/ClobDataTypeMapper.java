package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

public class ClobDataTypeMapper {

	public static Clob map(Object value, String conf, Connection connection) throws SQLException{
		Clob clob = connection.createClob();
		clob.setString(1, value.toString());
		return clob;
	}

}
