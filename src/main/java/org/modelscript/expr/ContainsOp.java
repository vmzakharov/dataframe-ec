package org.modelscript.expr;

import org.modelscript.expr.value.BooleanValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.VectorValue;

public interface ContainsOp
extends BinaryOp
{
    ContainsOp IN = new ContainsOp()
    {
        @Override
        public BooleanValue apply(Value value, VectorValue vectorValue)
        {
            return BooleanValue.valueOf(vectorValue.getElements().anySatisfy(e -> e.compareTo(value) == 0));
        }

        @Override
        public String asString()
        {
            return "in";
        }
    };

    @Override
    default Value apply(Value operand1, Value operand2)
    {
        return this.apply(operand1, (VectorValue) operand2);
    }

    BooleanValue apply(Value value, VectorValue vectorValue);

    String asString();
}
