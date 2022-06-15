package io.github.vmzakharov.ecdataframe.dsl.value;

public enum ValueType
{
    BOOLEAN, DOUBLE, LONG, STRING, DATE, DATE_TIME, VOID, VECTOR, DATA_FRAME;

    public boolean isVoid()    { return this == VOID; }

    public boolean isBoolean() { return this == BOOLEAN; }

    public boolean isLong()    { return this == LONG; }

    public boolean isDouble()  { return this == DOUBLE; }

    public boolean isNumber()  { return this == LONG || this == DOUBLE; }

    public boolean isDate()  { return this == DATE; }

    public boolean isDateTime()  { return this == DATE_TIME; }

    public boolean isTemporal() { return this == DATE || this == DATE_TIME; }

    public boolean isString()  { return this == STRING; }

    public boolean isVector()  { return this == VECTOR; }

    public boolean isDataFrame()  { return this == DATA_FRAME; }
}
