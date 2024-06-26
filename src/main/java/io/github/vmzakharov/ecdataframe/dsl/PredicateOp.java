package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public interface PredicateOp
extends BinaryOp
{
    @Override
    default BooleanValue apply(Value operand1, Value operand2)
    {
        if (operand1.isVoid() || operand2.isVoid())
        {
            throw this.unsupportedOn("null values");
        }

        return operand1.applyPredicate(operand2, this);
    }

    default BooleanValue applyString(String operand1, String operand2)
    {
        throw this.unsupportedOn("strings");
    }

    default BooleanValue applyDecimal(BigDecimal operand1, BigDecimal operand2)
    {
        throw this.unsupportedOn("decimals");
    }

    default BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
    {
        throw this.unsupportedOn("dates");
    }

    default BooleanValue applyDateTime(LocalDateTime operand1, LocalDateTime operand2)
    {
        throw this.unsupportedOn("datetime values");
    }

    default BooleanValue applyLong(long operand1, long operand2)
    {
        throw this.unsupportedOn("longs");
    }

    default BooleanValue applyDouble(double operand1, double operand2)
    {
        throw this.unsupportedOn("doubles");
    }

    default RuntimeException unsupportedOn(String type)
    {
        return exceptionByKey("DSL_OP_NOT_SUPPORTED")
                .with("operation", this.asString())
                .with("type", type)
                .getUnsupported();
    }
}
