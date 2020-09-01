package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.ComparisonOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

public class DoubleValue
extends AbstractValue
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
    public Value apply(UnaryOp operation)
    {
        return operation.applyDouble(this.value);
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
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return other.isVoid() ? 1 : Double.compare(this.doubleValue(), ((DoubleValue) other).doubleValue());
    }
}
