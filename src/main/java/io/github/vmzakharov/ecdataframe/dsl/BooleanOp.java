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
            Value op1Value = operand1.get();

            if (op1Value.isVoid())
            {
                Value op2Value = operand2.get();
                if (op2Value.isVoid())
                {
                    return Value.VOID;
                }
                else
                {
                    return ((BooleanValue) op2Value).isFalse() ? BooleanValue.FALSE : Value.VOID;
                }
            }

            return ((BooleanValue) op1Value).isFalse() ?  BooleanValue.FALSE : operand2.get();
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
            Value op1Value = operand1.get();

            if (op1Value.isVoid())
            {
                Value op2Value = operand2.get();
                if (op2Value.isVoid())
                {
                    return Value.VOID;
                }
                else
                {
                    return ((BooleanValue) op2Value).isTrue() ? BooleanValue.TRUE : Value.VOID;
                }
            }

            return ((BooleanValue) op1Value).isTrue() ? BooleanValue.TRUE : operand2.get();
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
        if (operand1.isVoid() || operand2.isVoid())
        {
            return Value.VOID;
        }

        return this.apply((BooleanValue) operand1, (BooleanValue) operand2);
    }

    BooleanValue apply(BooleanValue operand1, BooleanValue operand2);
}
