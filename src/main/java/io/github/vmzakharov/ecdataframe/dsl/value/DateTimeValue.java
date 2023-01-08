package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.util.ErrorReporter;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeValue
extends AbstractValue
{
    static private final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    private final LocalDateTime value;

    public DateTimeValue(LocalDateTime newValue)
    {
        ErrorReporter.reportAndThrowIf(newValue == null, "DateTime value cannot contain null, a void value should be used instead");
        this.value = newValue;
    }

    @Override
    public String asStringLiteral()
    {
        return this.value.format(this.getFormatter());
    }

    @Override
    public Value apply(UnaryOp operation)
    {
        return operation.applyDateTime(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        return operation.applyDateTime(this.dateTimeValue(), ((DateTimeValue) another).dateTimeValue());
    }

    private DateTimeFormatter getFormatter()
    {
        return FORMATTER;
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DATE_TIME;
    }

    public LocalDateTime dateTimeValue()
    {
        return this.value;
    }

    @Override
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);

        DateTimeValue otherDate = (DateTimeValue) other;

        if (this.dateTimeValue() == null)
        {
            return otherDate.dateTimeValue() == null ? 0 : -1;
        }

        if (otherDate.dateTimeValue() == null)
        {
            return 1;
        }

        return other.isVoid() ? 1 : this.dateTimeValue().compareTo(otherDate.dateTimeValue());
    }
}
