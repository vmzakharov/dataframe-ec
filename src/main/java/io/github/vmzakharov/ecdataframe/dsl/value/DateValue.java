package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record DateValue(LocalDate value)
implements Value
{
    static private final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

    public DateValue
    {
        this.throwExceptionIfNull(value);
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
        return other.isVoid() ? 1 : this.dateValue().compareTo(((DateValue) other).dateValue());
    }
}
