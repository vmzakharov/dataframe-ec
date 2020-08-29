package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

public interface BinaryOp
{
    Value apply(Value operand1, Value operand2);
    String asString();
}
