package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dataframe.ErrorReporter;
import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateValue
extends AbstractValue
{
    static private final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

    private final LocalDate value;

    public DateValue(LocalDate newValue)
    {
        ErrorReporter.reportAndThrow(newValue == null, "Date value cannot contain null, a void value should be used instead");
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
        return operation.applyDate(this.value);
    }

    @Override
    public BooleanValue applyPredicate(Value another, PredicateOp operation)
    {
        return operation.applyDate(this.dateValue(), ((DateValue) another).dateValue());
    }

    private DateTimeFormatter getFormatter()
    {
        return FORMATTER;
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
    public int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);

        DateValue otherDate = (DateValue) other;

        if (this.dateValue() == null)
        {
            return otherDate.dateValue() == null ? 0 : -1;
        }

        if (otherDate.dateValue() == null)
        {
            return 1;
        }

        return other.isVoid() ? 1 : this.dateValue().compareTo(otherDate.dateValue());
    }
}
