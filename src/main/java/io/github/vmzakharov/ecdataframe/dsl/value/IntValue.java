package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.math.BigDecimal;

public class IntValue
extends AbstractValue
implements WholeNumberValue
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

    @Override
    public long longValue()
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
        if (another.isWholeNumber())
        {
            return operation.applyLong(this.longValue(), ((WholeNumberValue) another).longValue());
        }

        return operation.applyDouble(this.doubleValue(), ((NumberValue) another).doubleValue());
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyInt(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        if (another.isWholeNumber())
        {
            return operation.applyLong(this.longValue(), ((WholeNumberValue) another).longValue());
        }

        return operation.applyDouble(this.doubleValue(), ((NumberValue) another).doubleValue());
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
