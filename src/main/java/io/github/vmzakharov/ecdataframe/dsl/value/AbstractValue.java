package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.util.ErrorReporter;

abstract public class AbstractValue
implements Value
{

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + ">" + this.asStringLiteral();
    }

    public void checkSameTypeForComparison(Value other)
    {
        if (null == other)
        {
            throw ErrorReporter.unsupported("Cannot compare a " + this.getClass().getName() + " to null");
        }

        if (!other.isVoid() && (this.getClass() != other.getClass()))
        {
            throw ErrorReporter.unsupported("Cannot compare a " + this.getClass().getName() + " to a " + other.getClass().getName());
        }
    }
}
