package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.PredicateOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public record DateTimeValue(LocalDateTime value)
implements Value
{
    static private final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    public DateTimeValue
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
        return other.isVoid() ? 1 : this.dateTimeValue().compareTo(((DateTimeValue) other).dateTimeValue());
    }
}
