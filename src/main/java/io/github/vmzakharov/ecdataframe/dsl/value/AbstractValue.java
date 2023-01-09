package io.github.vmzakharov.ecdataframe.dsl.value;

import static io.github.vmzakharov.ecdataframe.util.ErrorReporter.exception;

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
            throw exception("Cannot compare a ${className} to null")
                    .with("className",  this.getClass().getName()).getUnsupported();
        }

        if (!other.isVoid() && (this.getClass() != other.getClass()))
        {
            throw exception("Cannot compare a ${className} to a ${otherClassName}")
                    .with("thisClassName",  this.getClass().getName())
                    .with("otherClassName", other.getClass().getName())
                    .getUnsupported();
        }
    }
}
