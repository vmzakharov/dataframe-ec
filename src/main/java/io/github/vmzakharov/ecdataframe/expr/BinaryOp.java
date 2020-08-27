package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.Value;

public interface BinaryOp
{
    Value apply(Value operand1, Value operand2);
    String asString();
}
