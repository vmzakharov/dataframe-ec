package org.modelscript.expr.value;

import org.modelscript.expr.ArithmeticOp;

abstract public class BooleanValue
implements Value
{
    static public BooleanValue TRUE = new BooleanValue()
    {
        @Override
        public boolean isTrue()
        {
            return true;
        }

        @Override
        public String asStringLiteral()
        {
            return "TRUE";
        }
    };

    static public BooleanValue FALSE = new BooleanValue()
    {
        @Override
        public boolean isTrue()
        {
            return false;
        }

        @Override
        public String asStringLiteral()
        {
            return "FALSE";
        }
    };

    public static BooleanValue valueOf(boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        throw new UnsupportedOperationException("Cannot apply " + operation.asString() + " to a boolean");
    }

    abstract public boolean isTrue();

    @Override
    public ValueType getType()
    {
        return ValueType.BOOLEAN;
    }
}
