package org.modelscript.expr.value;

import org.modelscript.expr.ArithmeticOp;
import org.modelscript.expr.ComparisonOp;

public class DoubleValue
implements NumberValue
{
    private final double value;

    public DoubleValue(double newValue)
    {
        this.value = newValue;
    }

    public double doubleValue()
    {
        return this.value;
    }

    @Override
    public String asStringLiteral()
    {
        return Double.toString(this.value);
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        return operation.applyDouble(this.doubleValue(), ((NumberValue) another).doubleValue());
    }

    @Override
    public BooleanValue applyComparison(Value another, ComparisonOp operation)
    {
        return operation.applyDouble(this.doubleValue(), ((NumberValue) another).doubleValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DOUBLE;
    }

    @Override
    public String toString()
    {
        return this.asStringLiteral();
    }
}
