package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ListIterable;

import java.time.LocalDate;
import java.time.LocalDateTime;

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

    UnaryOp IS_EMPTY = new UnaryOp()
    {
        @Override
        public BooleanValue applyDate(LocalDate operand)
        {
            return BooleanValue.valueOf(operand == null);
        }

        @Override
        public BooleanValue applyString(String operand)
        {
            return BooleanValue.valueOf(operand == null || operand.isEmpty());
        }

        @Override
        public BooleanValue applyVector(ListIterable<Value> operand)
        {
            return BooleanValue.valueOf(operand == null || operand.isEmpty());
        }

        @Override
        public String asString()
        {
            return "is empty";
        }

        @Override
        public boolean isPrefix()
        {
            return false;
        }
    };

    UnaryOp IS_NOT_EMPTY = new UnaryOp()
    {
        @Override
        public BooleanValue applyDate(LocalDate operand)
        {
            return BooleanValue.valueOf(operand != null);
        }

        @Override
        public BooleanValue applyString(String operand)
        {
            return BooleanValue.valueOf(operand != null && operand.length() > 0);
        }

        @Override
        public BooleanValue applyVector(ListIterable<Value> operand)
        {
            return BooleanValue.valueOf(operand != null && operand.size() > 0);
        }

        @Override
        public String asString()
        {
            return "is not empty";
        }

        @Override
        public boolean isPrefix()
        {
            return false;
        }
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

    default BooleanValue applyDate(LocalDate operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a date");
    }

    default BooleanValue applyDateTime(LocalDateTime operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a datetime");
    }

    default BooleanValue applyString(String operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a string");
    }

    default BooleanValue applyVector(ListIterable<Value> operand)
    {
        throw new UnsupportedOperationException("Operation " + this.asString() + " cannot be applied to a vector");
    }

    default Value apply(Value operand)
    {
        return operand.apply(this);
    }

    default boolean isPrefix()
    {
        return true;
    }
}
