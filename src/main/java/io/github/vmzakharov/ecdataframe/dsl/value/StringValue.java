package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.ComparisonOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

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
    public Value apply(UnaryOp operation)
    {
        return operation.applyString(this.value);
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
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
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

        if (this.stringValue() == null)
        {
            return other.stringValue() == null ? 0 : -1;
        }

        if (other.stringValue() == null)
        {
            return 1;
        }

        return other.isVoid() ? 1 : this.stringValue().compareTo(other.stringValue());
    }
}
