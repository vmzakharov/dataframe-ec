package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ComparisonOp;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateValue
extends AbstractValue
{
    static private final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

    private final LocalDate value;

    public DateValue(LocalDate newValue)
    {
        this.value = newValue;
    }

    @Override
    public String asStringLiteral()
    {
        return this.value.format(this.getFormatter());
    }

    @Override
    public BooleanValue applyComparison(Value another, ComparisonOp operation)
    {
        return operation.applyDate(this.dateValue(), ((DateValue) another).dateValue());
    }

    private DateTimeFormatter getFormatter()
    {
        return this.FORMATTER;
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DATE;
    }

    public LocalDate dateValue()
    {
        return this.value;
    }

    @Override
    public int compareTo(Value o)
    {
        return 0;
    }
}
