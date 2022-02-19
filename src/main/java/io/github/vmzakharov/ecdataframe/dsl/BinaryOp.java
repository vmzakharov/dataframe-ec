package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.util.function.Supplier;

public interface BinaryOp
{
    Value apply(Value operand1, Value operand2);

    /**
     * Override this method if partial evaluation of operands is possible to determine the outcome of the operation,
     * e.g. "and" and "or" operators.
     * @param operand1 a supplier of the first value
     * @param operand2 a supplier of the second value
     * @return the result of applying the operation
     */
    default Value apply(Supplier<Value> operand1, Supplier<Value> operand2)
    {
        return this.apply(operand1.get(), operand2.get());
    }

    String asString();
}
