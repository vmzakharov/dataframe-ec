package io.github.vmzakharov.ecdataframe.expr.value;

import io.github.vmzakharov.ecdataframe.expr.ArithmeticOp;

abstract public class BooleanValue
extends AbstractValue
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

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return Boolean.compare(this.isTrue(), ((BooleanValue) other).isTrue());
    }
}
