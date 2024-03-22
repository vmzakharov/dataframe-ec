package io.github.vmzakharov.ecdataframe.dsl.value;

public enum ValueType
{
    BOOLEAN, DOUBLE, FLOAT, LONG, INT, DECIMAL, STRING, DATE, DATE_TIME, VOID, VECTOR, DATA_FRAME;

    public boolean isVoid()
    {
        return this == VOID;
    }

    public boolean isBoolean()
    {
        return this == BOOLEAN;
    }

    public boolean isLong()
    {
        return this == LONG;
    }

    public boolean isInt()
    {
        return this == INT;
    }

    public boolean isDouble()
    {
        return this == DOUBLE;
    }

    public boolean isFloat()
    {
        return this == FLOAT;
    }

    public boolean isDecimal()
    {
        return this == DECIMAL;
    }

    public boolean isNumber()
    {
        return this == LONG || this == DOUBLE || this == INT || this == FLOAT;
    }

    public boolean isDate()
    {
        return this == DATE;
    }

    public boolean isDateTime()
    {
        return this == DATE_TIME;
    }

    public boolean isTemporal()
    {
        return this == DATE || this == DATE_TIME;
    }

    public boolean isString()
    {
        return this == STRING;
    }

    public boolean isVector()
    {
        return this == VECTOR;
    }

    public boolean isDataFrame()
    {
        return this == DATA_FRAME;
    }
}
