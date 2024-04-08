package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public interface BooleanValue
extends Value
{
    BooleanValue TRUE = new BooleanTrue();

    BooleanValue FALSE = new BooleanFalse();

    static BooleanValue valueOf(boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @Override
    default Value apply(Value another, ArithmeticOp operation)
    {
        throw exceptionByKey("DSL_OP_NOT_SUPPORTED")
                .with("operation", operation.asString())
                .with("type", this.getType().toString())
                .getUnsupported();
    }

    @Override
    default Value apply(UnaryOp operation)
    {
        return operation.applyBoolean(this.isTrue());
    }

    boolean isTrue();

    boolean isFalse();

    boolean is(boolean booleanValue);

    @Override
    default ValueType getType()
    {
        return ValueType.BOOLEAN;
    }

    @Override
    default int compareTo(Value other)
    {
        this.checkSameTypeForComparison(other);
        return Boolean.compare(this.isTrue(), ((BooleanValue) other).isTrue());
    }

    record BooleanTrue()
    implements BooleanValue
    {
        @Override
        public boolean isTrue()
        {
            return true;
        }

        @Override
        public boolean isFalse()
        {
            return false;
        }

        @Override
        public boolean is(boolean booleanValue)
        {
            return booleanValue;
        }

        @Override
        public String asStringLiteral()
        {
            return "TRUE";
        }
    }

    record BooleanFalse()
    implements BooleanValue
    {
        @Override
        public boolean isTrue()
        {
            return false;
        }

        @Override
        public boolean isFalse()
        {
            return true;
        }

        @Override
        public boolean is(boolean booleanValue)
        {
            return !booleanValue;
        }

        @Override
        public String asStringLiteral()
        {
            return "FALSE";
        }
    }
}
