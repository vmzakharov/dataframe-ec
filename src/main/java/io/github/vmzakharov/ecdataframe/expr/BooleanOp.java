package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.expr.value.Value;

public interface BooleanOp
extends BinaryOp
{
    BooleanOp AND = new BooleanOp()
    {
        @Override
        public BooleanValue apply(BooleanValue operand1, BooleanValue operand2)
        {
            return BooleanValue.valueOf(operand1.isTrue() && operand2.isTrue());
        }

        @Override
        public String asString() { return "AND"; }
    };

    BooleanOp OR = new BooleanOp()
    {
        @Override
        public BooleanValue apply(BooleanValue operand1, BooleanValue operand2)
        {
            return BooleanValue.valueOf(operand1.isTrue() || operand2.isTrue());
        }

        @Override
        public String asString()
        {
            return "OR";
        }
    };

    BooleanOp XOR = new BooleanOp()
    {
        @Override
        public BooleanValue apply(BooleanValue operand1, BooleanValue operand2)
        {
            return BooleanValue.valueOf(operand1.isTrue() ^ operand2.isTrue()) ;
        }

        @Override
        public String asString()
        {
            return "XOR";
        }
    };

    @Override
    default Value apply(Value operand1, Value operand2)
    {
        return this.apply((BooleanValue) operand1, (BooleanValue) operand2);
    }

    BooleanValue apply(BooleanValue operand1, BooleanValue operand2);

    String asString();
}
