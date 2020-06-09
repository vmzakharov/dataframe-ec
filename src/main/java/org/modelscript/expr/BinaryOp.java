package org.modelscript.expr;

import org.modelscript.expr.value.Value;

public interface BinaryOp
{
    Value apply(Value operand1, Value operand2);
    String asString();
}
