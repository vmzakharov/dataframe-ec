package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public interface PredicateOp
extends BinaryOp
{
    default BooleanValue apply(Value operand1, Value operand2)
    {
        if (operand1.isVoid() || operand2.isVoid())
        {
            throw new NullPointerException("Cannot apply " + this.asString() + " operation to null values");
        }
        return operand1.applyPredicate(operand2, this);
    }

    default BooleanValue applyString(String operand1, String operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to strings");
    }

    default BooleanValue applyDecimal(BigDecimal operand1, BigDecimal operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to decimals");
    }

    default BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to dates");
    }

    default BooleanValue applyDateTime(LocalDateTime operand1, LocalDateTime operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to datetime values");
    }

    default BooleanValue applyLong(long operand1, long operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to longs");
    }

    default BooleanValue applyDouble(double operand1, double operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to doubles");
    }
}
