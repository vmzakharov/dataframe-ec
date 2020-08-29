package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

public interface ArithmeticOp
extends BinaryOp
{
    ArithmeticOp ADD = new ArithmeticOp()
    {
        @Override
        public StringValue applyString(String operand1, String operand2) { return new StringValue(operand1 + operand2);
        }
        @Override
        public LongValue applyLong(long operand1, long operand2) { return new LongValue(operand1 + operand2); }

        @Override
        public DoubleValue applyDouble(double operand1, double operand2) { return new DoubleValue(operand1 + operand2); }

        @Override
        public String asString() { return "+"; }
    };

    ArithmeticOp MULTIPLY = new ArithmeticOp()
    {
        @Override
        public LongValue applyLong(long operand1, long operand2)
        {
            return new LongValue(operand1 * operand2);
        }

        @Override
        public DoubleValue applyDouble(double operand1, double operand2) { return new DoubleValue(operand1 * operand2); }

        @Override
        public String asString()
        {
            return "*";
        }
    };

    ArithmeticOp SUBTRACT = new ArithmeticOp()
    {
        @Override
        public LongValue applyLong(long operand1, long operand2)
        {
            return new LongValue(operand1 - operand2);
        }

        @Override
        public DoubleValue applyDouble(double operand1, double operand2) { return new DoubleValue(operand1 - operand2); }

        @Override
        public String asString()
        {
            return "-";
        }
    };

    ArithmeticOp DIVIDE = new ArithmeticOp()
    {
        @Override
        public LongValue applyLong(long operand1, long operand2)
        {
            return new LongValue(operand1 / operand2);
        }

        @Override
        public DoubleValue applyDouble(double operand1, double operand2) { return new DoubleValue(operand1 / operand2); }

        @Override
        public String asString()
        {
            return "/";
        }
    };

    default Value apply(Value operand1, Value operand2)
    {
        return operand1.apply(operand2, this);
    }

    default StringValue applyString(String operand1, String operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to String");
    }

    LongValue applyLong(long operand1, long operand2);

    DoubleValue applyDouble(double operand1, double operand2);

    String asString();
}
