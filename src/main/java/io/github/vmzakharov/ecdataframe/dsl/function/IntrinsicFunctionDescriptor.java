package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.FunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public abstract class IntrinsicFunctionDescriptor
implements FunctionDescriptor
{
    private final String name;
    private final String normalizedName;
    private ListIterable<String> parameterNames;

    public IntrinsicFunctionDescriptor(String newName, ListIterable<String> newParameterNames)
    {
        this.name = newName;
        this.normalizedName = this.name.toUpperCase();
        this.setParameterNames(newParameterNames);
    }

    public IntrinsicFunctionDescriptor(String newName)
    {
        this(newName, Lists.immutable.empty());
    }

    public String getName()
    {
        return this.name;
    }

    public String getNormalizedName()
    {
        return this.normalizedName;
    }

    protected void setParameterNames(ListIterable<String> newParameterNames)
    {
        this.parameterNames = newParameterNames;
    }

    public ListIterable<String> getParameterNames()
    {
        return this.parameterNames;
    }

    public boolean hasExplicitParameters()
    {
        return this.parameterNames.notEmpty();
    }

    public ValueType returnType(ListIterable<ValueType> parameterTypes)
    {
        return ValueType.VOID;
    }

    abstract public Value evaluate(EvalContext context);

     protected VectorValue getParameterVectorFrom(EvalContext context)
     {
         return (VectorValue) context.getVariableOrDefault(this.magicalParameterName(), VectorValue.EMPTY);
     }

    public String usageString()
    {
        return "Usage: " + this.name + "(" + this.parameterNames.makeString(", ") + ")";
    }

    public String magicalParameterName()
    {
        return "*" + this.getName();
    }

    protected void assertParameterCount(int expectedCount, int actualCount)
    {
        if (expectedCount != actualCount)
        {
            throw this.invalidParameterCountException(expectedCount, actualCount);
        }
    }

    protected RuntimeException invalidParameterCountException(int expectedCount, int actualCount)
    {
        return exceptionByKey("DSL_INVALID_PARAM_COUNT")
                .with("functionName", this.getName())
                .with("usageString", this.usageString())
                .get();
    }

    protected void assertParameterType(ValueType expected, ValueType actual)
    {
        if (expected != actual)
        {
            this.throwInvalidParameterException(actual);
        }
    }

    protected void assertParameterType(ListIterable<ValueType> expected, ValueType actual)
    {
        if (!expected.contains(actual))
        {
            this.throwInvalidParameterException(actual);
        }
    }

    protected void throwInvalidParameterException(ValueType actualType)
    {
        exceptionByKey("DSL_INVALID_PARAM_TYPE")
                .with("type", actualType.toString())
                .with("functionName", this.getName())
                .with("usageString", this.usageString())
                .fire();
    }
}
