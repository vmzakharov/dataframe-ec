package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ListIterable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public interface UnaryOp
{
    UnaryOp MINUS = new UnaryOp()
    {
        @Override
        public LongValue applyLong(long operand)
        {
            return new LongValue(-operand);
        }

        @Override
        public IntValue applyInt(int operand)
        {
            return new IntValue(-operand);
        }

        @Override
        public DoubleValue applyDouble(double operand)
        {
            return new DoubleValue(-operand);
        }

        @Override
        public FloatValue applyFloat(float operand)
        {
            return new FloatValue(-operand);
        }

        @Override
        public String asString()
        {
            return "-";
        }
    };

    UnaryOp NOT = new UnaryOp()
    {
        @Override
        public Value apply(Value operand)
        {
            return operand.isVoid() ? Value.VOID : UnaryOp.super.apply(operand);
        }

        @Override
        public BooleanValue applyBoolean(boolean operand)
        {
            return BooleanValue.valueOf(!operand);
        }

        @Override
        public String asString()
        {
            return "!";
        }
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
            return BooleanValue.valueOf(operand != null && !operand.isEmpty());
        }

        @Override
        public BooleanValue applyVector(ListIterable<Value> operand)
        {
            return BooleanValue.valueOf(operand != null && !operand.isEmpty());
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

    UnaryOp IS_NULL = new UnaryOp()
    {
        @Override
        public Value apply(Value operand)
        {
            return BooleanValue.valueOf(operand.isVoid());
        }

        @Override
        public String asString()
        {
            return "is null";
        }

        @Override
        public boolean isPrefix()
        {
            return false;
        }
    };

    UnaryOp IS_NOT_NULL = new UnaryOp()
    {
        @Override
        public Value apply(Value operand)
        {
            return BooleanValue.valueOf(!operand.isVoid());
        }

        @Override
        public String asString()
        {
            return "is not null";
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
        throw this.unsupportedOn("boolean");
    }

    default LongValue applyLong(long operand)
    {
        throw this.unsupportedOn("long");
    }

    default IntValue applyInt(int operand)
    {
        throw this.unsupportedOn("int");
    }

    default DoubleValue applyDouble(double operand)
    {
        throw this.unsupportedOn("double");
    }

    default FloatValue applyFloat(float operand)
    {
        throw this.unsupportedOn("float");
    }

    default BooleanValue applyDate(LocalDate operand)
    {
        throw this.unsupportedOn("date");
    }

    default BooleanValue applyDateTime(LocalDateTime operand)
    {
        throw this.unsupportedOn("datetime");
    }

    default BooleanValue applyString(String operand)
    {
        throw this.unsupportedOn("string");
    }

    default BooleanValue applyDecimal(BigDecimal operand)
    {
        throw this.unsupportedOn("decimal");
    }

    default BooleanValue applyVector(ListIterable<Value> operand)
    {
        throw this.unsupportedOn("vector");
    }

    default Value apply(Value operand)
    {
        return operand.apply(this);
    }

    default boolean isPrefix()
    {
        return true;
    }

    default RuntimeException unsupportedOn(String type)
    {
        return exceptionByKey("DSL_OP_NOT_SUPPORTED")
                .with("operation", this.asString())
                .with("type", type).getUnsupported();
    }
}
