package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

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

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyBoolean(this.isTrue());
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
