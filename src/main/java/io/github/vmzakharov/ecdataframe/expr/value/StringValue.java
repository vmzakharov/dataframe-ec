package io.github.vmzakharov.ecdataframe.expr.value;

import io.github.vmzakharov.ecdataframe.expr.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.expr.ComparisonOp;

public class StringValue
extends AbstractValue
{
    private final String value;

    public StringValue(String newValue)
    {
        this.value = newValue;
    }

    @Override
    public Value apply(Value another, ArithmeticOp operation)
    {
        return operation.applyString(this.stringValue(), another.stringValue());
    }

    @Override
    public String asStringLiteral()
    {
        return '"' + this.value + '"';
    }

    @Override
    public String stringValue()
    {
        return this.value;
    }

    @Override
    public BooleanValue applyComparison(Value another, ComparisonOp operation)
    {
        return operation.applyString(this.stringValue(), ((StringValue) another).stringValue());
    }

    @Override
    public ValueType getType()
    {
        return ValueType.STRING;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return other.isVoid() ? 1 : this.stringValue().compareTo(other.stringValue());
    }
}
