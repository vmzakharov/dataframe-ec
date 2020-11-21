package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.time.LocalDate;

public interface ComparisonOp
extends PredicateOp
{
    ComparisonOp EQ = new ComparisonOp()
    {
        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            if (operand1 == null)
            {
                return BooleanValue.valueOf(operand2 == null);
            }

            return BooleanValue.valueOf(operand1.equals(operand2));
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand1 == null)
            {
                return BooleanValue.valueOf(operand2 == null);
            }

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
            if (operand1 == null)
            {
                return BooleanValue.valueOf(operand2 != null);
            }

            return BooleanValue.valueOf(!operand1.equals(operand2));
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand1 == null)
            {
                return BooleanValue.valueOf(operand2 != null);
            }

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
            if (operand2 == null)
            {
                return BooleanValue.FALSE;
            }

            if (operand1 == null)
            {
                return BooleanValue.TRUE;
            }

            return BooleanValue.valueOf(operand1.compareTo(operand2) < 0);
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand2 == null)
            {
                return BooleanValue.FALSE;
            }

            if (operand1 == null)
            {
                return BooleanValue.TRUE;
            }

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
            if (operand1 == null)
            {
                return BooleanValue.TRUE;
            }

            if (operand2 == null)
            {
                return BooleanValue.FALSE;
            }

            return BooleanValue.valueOf(operand1.compareTo(operand2) <= 0);
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand1 == null)
            {
                return BooleanValue.TRUE;
            }

            if (operand2 == null)
            {
                return BooleanValue.FALSE;
            }

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
            if (operand1 == null)
            {
                return BooleanValue.FALSE;
            }

            if (operand2 == null)
            {
                return BooleanValue.TRUE;
            }

            return BooleanValue.valueOf(operand1.compareTo(operand2) > 0);
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand1 == null)
            {
                return BooleanValue.FALSE;
            }

            if (operand2 == null)
            {
                return BooleanValue.TRUE;
            }

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
            if (operand2 == null)
            {
                return BooleanValue.TRUE;
            }

            if (operand1 == null)
            {
                return BooleanValue.FALSE;
            }

            return BooleanValue.valueOf(operand1.compareTo(operand2) >= 0);
        }

        @Override
        public BooleanValue applyDate(LocalDate operand1, LocalDate operand2)
        {
            if (operand2 == null)
            {
                return BooleanValue.TRUE;
            }

            if (operand1 == null)
            {
                return BooleanValue.FALSE;
            }

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
}
