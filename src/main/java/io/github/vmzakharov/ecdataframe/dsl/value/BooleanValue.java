package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

/**
 * The DSL value of boolean type (true, false)
 */
public interface BooleanValue
extends Value
{
    BooleanValue TRUE = new BooleanTrue();

    BooleanValue FALSE = new BooleanFalse();

    /**
     * A factory method returning the instance of {@code BooleanValue} with the same truth value as the parameter, that
     * is {@code TRUE} for {@code boolean} {@code true} and {@code FALSE} for {@code boolean} {@code false}
     * @param value the value for which the corresponding instance of this class will be returned
     * @return the instance of {@code BooleanValue} with the same truth value as the parameter
     */
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

    /**
     * checks if this instance of {@code BooleanValue} is true
     * @return true if this instance is true, false otherwise
     */
    boolean isTrue();

    /**
     * checks if this instance of {@code BooleanValue} is false
     * @return true if this instance is false, false otherwise
     */
    boolean isFalse();

    /**
     * checks if this instance of {@code BooleanValue} has the same truth value as the parameter
     * @param booleanValue the value to compare with
     * @return true if this instance and the parameter have the same truth value, false otherwise
     */
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

    /**
     * Represents the boolean value of {@code true}, effectively a singleton.
     */
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

    /**
     * Represents the boolean value of {@code true}, effectively a singleton.
     */
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
