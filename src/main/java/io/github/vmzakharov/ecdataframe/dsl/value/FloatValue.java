package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.math.BigDecimal;

public record FloatValue(float value)
implements RealNumberValue
{
    public float floatValue()
    {
        return this.value;
    }

    @Override
    public double doubleValue()
    {
        return this.value;
    }

    @Override
    public BigDecimal decimalValue()
    {
        return BigDecimal.valueOf(this.value);
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
        return operation.applyFloat(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        return operation.applyDouble(this.doubleValue(), ((NumberValue) another).doubleValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.FLOAT;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return other.isVoid() ? 1 : Float.compare(this.floatValue(), ((FloatValue) other).floatValue());
    }
}
