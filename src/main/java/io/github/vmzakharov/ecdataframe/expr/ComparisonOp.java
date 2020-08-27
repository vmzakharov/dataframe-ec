package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.expr.value.Value;

public interface ComparisonOp
extends BinaryOp
{
    ComparisonOp EQ = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand1.equals(operand2));
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 == operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 == operand2);
        }

        @Override
        public String asString() { return "=="; }
    };

    ComparisonOp NE = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(!operand1.equals(operand2));
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 != operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 != operand2);
        }

        @Override
        public String asString() { return "!="; }
    };

    ComparisonOp LT = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand1.compareTo(operand2) < 0);
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 < operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 < operand2);
        }

        @Override
        public String asString() { return "<"; }
    };

    ComparisonOp LTE = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand1.compareTo(operand2) <= 0);
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 <= operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 <= operand2);
        }

        @Override
        public String asString() { return "<="; }
    };

    ComparisonOp GT = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand1.compareTo(operand2) > 0);
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 > operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 > operand2);
        }

        @Override
        public String asString() { return ">"; }
    };

    ComparisonOp GTE = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand1.compareTo(operand2) >= 0);
        }
        @Override
        public BooleanValue applyLong(long operand1, long operand2)
        {
            return BooleanValue.valueOf(operand1 >= operand2);
        }

        @Override
        public BooleanValue applyDouble(double operand1, double operand2)
        {
            return BooleanValue.valueOf(operand1 >= operand2);
        }

        @Override
        public String asString() { return ">="; }
    };

    default BooleanValue apply(Value operand1, Value operand2)
    {
        return operand1.applyComparison(operand2, this);
    }

    BooleanValue applyString(String operand1, String operand2);

    BooleanValue applyLong(long operand1, long operand2);

    BooleanValue applyDouble(double operand1, double operand2);

    String asString();
}
