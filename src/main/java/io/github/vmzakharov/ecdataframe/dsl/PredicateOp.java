package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.time.LocalDate;

public interface PredicateOp
extends BinaryOp
{
    default BooleanValue apply(Value operand1, Value operand2)
    {
        return operand1.applyPredicate(operand2, this);
    }

    default BooleanValue applyString(String operand1, String operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to strings");
    }

    default BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to dates");
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
