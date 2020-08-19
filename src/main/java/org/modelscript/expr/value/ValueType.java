package org.modelscript.expr.value;

public enum ValueType
{
    BOOLEAN, DOUBLE, LONG, NUMBER, STRING, VOID, VECTOR, DATA_FRAME;

    public boolean isVoid()    { return this == VOID; }
    public boolean isBoolean() { return this == BOOLEAN; }
    public boolean isLong()    { return this == LONG; }
    public boolean isDouble()  { return this == DOUBLE; }
    public boolean isNumber()  { return this == NUMBER || this == LONG || this == DOUBLE; }
    public boolean isString()  { return this == STRING; }
    public boolean isVector()  { return this == VECTOR; }
    public boolean isDataFrame()  { return this == DATA_FRAME; }
}
