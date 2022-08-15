package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.util.function.Supplier;

public interface BooleanOp
extends BinaryOp
{
    BooleanOp AND = new BooleanOp()
    {
        @Override
        public Value apply(Supplier<Value> operand1, Supplier<Value> operand2)
        {
            BooleanValue op1Value = (BooleanValue) operand1.get();
            return op1Value.isTrue() ? operand2.get() : BooleanValue.FALSE;
        }

        @Override
        public BooleanValue apply(BooleanValue operand1, BooleanValue operand2)
        {
            return BooleanValue.valueOf(operand1.isTrue() && operand2.isTrue());
        }

        @Override
        public String asString()
        {
            return "AND";
        }
    };

    BooleanOp OR = new BooleanOp()
    {
        @Override
        public Value apply(Supplier<Value> operand1, Supplier<Value> operand2)
        {
            BooleanValue op1Value = (BooleanValue) operand1.get();
            return op1Value.isTrue() ? BooleanValue.TRUE : operand2.get();
        }

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
            return BooleanValue.valueOf(operand1.isTrue() ^ operand2.isTrue());
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
}
