package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.util.function.Supplier;

/**
 * A binary operation in the expression DSL
 */
public interface BinaryOp
{
    /**
     * Applies this operation to the operands
     * @param operand1 the first operand
     * @param operand2 the second operand
     * @return the result of applying this operation
     */
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

    /**
     * The parseable string representation of this operation parseable
     * @return the string representation of this operation
     */
    String asString();
}
