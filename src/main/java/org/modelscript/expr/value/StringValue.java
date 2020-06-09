package org.modelscript.expr.value;

import org.modelscript.expr.ArithmeticOp;
import org.modelscript.expr.ComparisonOp;

public class StringValue
implements Value
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
}
