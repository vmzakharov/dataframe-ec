package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

public record StringValue(String value)
implements Value
{
    public StringValue
    {
        this.throwExceptionIfNull(value);
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
        return this.value.indexOf('\"') == -1 ? '"' + this.value + '"' : '\'' + this.value + '\'';
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
        return other.isVoid() ? 1 : this.stringValue().compareTo(other.stringValue());
    }
}
