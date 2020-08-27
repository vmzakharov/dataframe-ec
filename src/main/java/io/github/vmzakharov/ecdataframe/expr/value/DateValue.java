package io.github.vmzakharov.ecdataframe.expr.value;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateValue
extends AbstractValue
{
    static private final DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;

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

    private DateTimeFormatter getFormatter()
    {
        return this.formatter;
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
