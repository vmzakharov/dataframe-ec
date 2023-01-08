package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.util.ErrorReporter;
import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.math.BigDecimal;

public class DecimalValue
extends AbstractValue
implements NumberValue
{
    private final BigDecimal value;

    public DecimalValue(BigDecimal newValue)
    {
        ErrorReporter.reportAndThrowIf(newValue == null, "Decimal value cannot contain null, a void value should be used instead");
        this.value = newValue;
    }

    public DecimalValue(long unscaledValue, int scale)
    {
        this.value = BigDecimal.valueOf(unscaledValue, scale);
    }

    @Override
    public String asStringLiteral()
    {
        return "[" + this.value.unscaledValue() + ',' + this.value.scale() + ']';
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        return operation.applyDecimal(this.decimalValue(), ((NumberValue) another).decimalValue());
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyDecimal(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        return operation.applyDecimal(this.decimalValue(), ((DecimalValue) another).decimalValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DECIMAL;
    }

    public BigDecimal decimalValue()
    {
        return this.value;
    }

    @Override
    public double doubleValue()
    {
        ErrorReporter.reportAndThrow("Cannot convert decimal value to floating point");
        return 0.0;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);

        DecimalValue otherDate = (DecimalValue) other;

        if (this.decimalValue() == null)
        {
            return otherDate.decimalValue() == null ? 0 : -1;
        }

        if (otherDate.decimalValue() == null)
        {
            return 1;
        }

        return other.isVoid() ? 1 : this.decimalValue().compareTo(otherDate.decimalValue());
    }
}
