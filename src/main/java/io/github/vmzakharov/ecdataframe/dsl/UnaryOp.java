package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

public interface UnaryOp
{
    UnaryOp MINUS = new UnaryOp()
    {
        @Override
        public LongValue applyLong(long operand) { return new LongValue(-operand); }

        @Override
        public DoubleValue applyDouble(double operand) { return new DoubleValue(-operand); }

        @Override
        public String asString() { return "-"; }
    };

    UnaryOp NOT = new UnaryOp()
    {
        @Override
        public BooleanValue applyBoolean(boolean operand)
        {
            return BooleanValue.valueOf(!operand);
        }

        public String asString() { return "!"; }
    };

    String asString();

    default BooleanValue applyBoolean(boolean operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a boolean");
    }

    default LongValue applyLong(long operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a long");
    }

    default DoubleValue applyDouble(double operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a double");
    }

    default Value apply(Value operand)
    {
        return operand.apply(this);
    }
}
