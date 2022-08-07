package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.math.BigDecimal;
import java.math.MathContext;

public interface ArithmeticOp
extends BinaryOp
{
    ArithmeticOp ADD = new ArithmeticOp()
    {
        @Override
        public Value applyString(String operand1, String operand2)
        {
            if (operand1 == null || operand2 == null)
            {
                return Value.VOID;
            }
            return new StringValue(operand1 + operand2);
        }

        @Override
        public Value applyLong(long operand1, long operand2) { return new LongValue(operand1 + operand2); }

        @Override
        public Value applyDouble(double operand1, double operand2) { return new DoubleValue(operand1 + operand2); }

        @Override
        public Value applyDecimal(BigDecimal operand1, BigDecimal operand2)
        {
            return new DecimalValue(operand1.add(operand2));
        }

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
        public Value applyDecimal(BigDecimal operand1, BigDecimal operand2)
        {
            return new DecimalValue(operand1.multiply(operand2));
        }

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
        public Value applyDecimal(BigDecimal operand1, BigDecimal operand2)
        {
            return new DecimalValue(operand1.subtract(operand2));
        }

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
        public Value applyDecimal(BigDecimal operand1, BigDecimal operand2)
        {
//            return new DecimalValue(operand1.divide(operand2, RoundingMode.HALF_UP));
            return new DecimalValue(operand1.divide(operand2, MathContext.DECIMAL64));
        }

        @Override
        public String asString()
        {
            return "/";
        }
    };

    default Value apply(Value operand1, Value operand2)
    {
        if (operand1.isVoid() || operand2.isVoid())
        {
            return Value.VOID;
        }

        return operand1.apply(operand2, this);
    }

    default Value applyString(String operand1, String operand2)
    {
        throw new UnsupportedOperationException("Cannot apply '" + this.asString() + "' to String");
    }

    Value applyLong(long operand1, long operand2);

    Value applyDouble(double operand1, double operand2);

    Value applyDecimal(BigDecimal operand1, BigDecimal operand2);
}
