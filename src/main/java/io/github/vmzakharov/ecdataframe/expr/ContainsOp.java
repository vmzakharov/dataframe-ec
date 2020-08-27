package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.expr.value.Value;
import io.github.vmzakharov.ecdataframe.expr.value.VectorValue;

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

    ContainsOp NOT_IN = new ContainsOp()
    {
        @Override
        public BooleanValue apply(Value value, VectorValue vectorValue)
        {
            return BooleanValue.valueOf(vectorValue.getElements().allSatisfy(e -> e.compareTo(value) != 0));
        }

        @Override
        public String asString()
        {
            return "not in";
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
