package io.github.vmzakharov.ecdataframe.dsl.value;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

abstract public class AbstractValue
implements Value
{

    public void throwExceptionIfNull(Object newValue)
    {
        if (newValue == null)
        {
            exceptionByKey("DSL_NULL_VALUE_NOT_ALLOWED").with("type", this.getType()).fire();
        }
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + ">" + this.asStringLiteral();
    }

    public void checkSameTypeForComparison(Value other)
    {
        if (null == other)
        {
            throw exceptionByKey("DSL_COMPARE_TO_NULL")
                    .with("className",  this.getClass().getSimpleName()).getUnsupported();
        }

        if (!other.isVoid() && (this.getClass() != other.getClass()))
        {
            throw exceptionByKey("DSL_COMPARE_INCOMPATIBLE")
                    .with("className",  this.getClass().getSimpleName())
                    .with("otherClassName", other.getClass().getSimpleName())
                    .getUnsupported();
        }
    }
}
