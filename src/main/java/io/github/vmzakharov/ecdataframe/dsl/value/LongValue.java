package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

public class LongValue
extends AbstractValue
implements NumberValue
{
    private final long value;

    public LongValue(long newValue)
    {
        this.value = newValue;
    }

    @Override
    public String asStringLiteral()
    {
        return Long.toString(this.value);
    }

    public long longValue()
    {
        return this.value;
    }

    @Override
    public double doubleValue()
    {
        return this.value;
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        if (another.isDouble())
        {
            return operation.applyDouble(this.doubleValue(), ((DoubleValue) another).doubleValue());
        }
        return operation.applyLong(this.longValue(), ((LongValue) another).longValue());
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyLong(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        if (another.isDouble())
        {
            return operation.applyDouble(this.doubleValue(), ((DoubleValue) another).doubleValue());
        }
        return operation.applyLong(this.longValue(), ((LongValue) another).longValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.LONG;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return other.isVoid() ? 1 : Long.compare(this.longValue(), ((LongValue) other).longValue());
    }
}
