package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.math.BigDecimal;

public class IntValue
extends AbstractValue
implements NumberValue
{
    private final int value;

    public IntValue(int newValue)
    {
        this.value = newValue;
    }

    @Override
    public String asStringLiteral()
    {
        return Integer.toString(this.value);
    }

    public int intValue()
    {
        return this.value;
    }

    @Override
    public double doubleValue()
    {
        return this.value;
    }

    public BigDecimal decimalValue()
    {
        return BigDecimal.valueOf(this.value);
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        if (another.isDouble())
        {
            return operation.applyDouble(this.doubleValue(), ((DoubleValue) another).doubleValue());
        }

        if (another.isLong())
        {
            return operation.applyLong(this.intValue(), ((LongValue) another).longValue());
        }

        return operation.applyLong(this.intValue(), ((IntValue) another).intValue());
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyInt(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        if (another.isDouble())
        {
            return operation.applyDouble(this.doubleValue(), ((DoubleValue) another).doubleValue());
        }

        if (another.isLong())
        {
            return operation.applyLong(this.intValue(), ((LongValue) another).longValue());
        }
        return operation.applyLong(this.intValue(), ((IntValue) another).intValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.INT;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return other.isVoid() ? 1 : Integer.compare(this.intValue(), ((IntValue) other).intValue());
    }
}
