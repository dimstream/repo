package com.vmware.dim.jdbc.datatype.mapping;


public enum DataType {

	L(LongDataTypeMapper.class),
	I(IntegerDataTypeMapper.class),
	S(StringDataTypeMapper.class),
	D(DateDataTypeMapper.class),
	T(TimestampDataTypeMapper.class),
	F(FloatDataTypeMapper.class),
	A(DoubleDataTypeMapper.class),
	B(BigDecimalDataTypeMapper.class),
	C(ClobDataTypeMapper.class);
	
	Class<?> class1;
	
	private DataType(Class<?> class1) {
		this.class1 = class1;
	}
	
	public Class<?> getMapperClass(){
		return this.class1;
	}
}
