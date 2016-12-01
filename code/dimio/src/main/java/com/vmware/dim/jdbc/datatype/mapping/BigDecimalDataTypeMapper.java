package com.vmware.dim.jdbc.datatype.mapping;

import java.math.BigDecimal;
import java.sql.Connection;

public class BigDecimalDataTypeMapper {

	public static BigDecimal map(Object value, String conf, Connection connection){
		BigDecimal bigDecimal = new BigDecimal(value.toString());
		return new BigDecimal(bigDecimal.toPlainString());
	}

}
