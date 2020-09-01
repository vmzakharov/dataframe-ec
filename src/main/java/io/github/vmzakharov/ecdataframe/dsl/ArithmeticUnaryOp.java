package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;

public interface ArithmeticUnaryOp
extends UnaryOp
{
    ArithmeticUnaryOp MINUS = new ArithmeticUnaryOp()
    {
        @Override
        public LongValue applyLong(long operand) { return new LongValue(-operand); }

        @Override
        public DoubleValue applyDouble(double operand) { return new DoubleValue(-operand); }

        @Override
        public String asString() { return "-"; }
    };
}
