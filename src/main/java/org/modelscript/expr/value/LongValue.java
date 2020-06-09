package org.modelscript.expr.value;

import org.modelscript.expr.ArithmeticOp;
import org.modelscript.expr.ComparisonOp;

public class LongValue
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
    public BooleanValue applyComparison(Value another, ComparisonOp operation)
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
}
