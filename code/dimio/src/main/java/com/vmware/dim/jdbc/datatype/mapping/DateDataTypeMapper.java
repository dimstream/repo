package com.vmware.dim.jdbc.datatype.mapping;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateDataTypeMapper {

	public static java.sql.Date map(Object value, String conf, Connection connection) throws ParseException {

		String dateConf = conf.substring(2, conf.length() - 1);
		String[] formatTimezone = dateConf.split("~");
		SimpleDateFormat inputFormat = new SimpleDateFormat(formatTimezone[0]);
		inputFormat.setTimeZone(TimeZone.getTimeZone(formatTimezone[1]));
		Date ipDate = inputFormat.parse(value.toString());
		java.sql.Date sqlDate = new java.sql.Date(ipDate.getTime());
		return sqlDate;
	}

	public static String getConfigurationAtIndex(String conf, int index) {
		String dateConf = conf.substring(2, conf.length() - 1);
		String[] formatTimezone = dateConf.split("~");

		if ((formatTimezone.length - 1) <= index) {
			return formatTimezone[index];
		}

		return "UTC";
	}
}
