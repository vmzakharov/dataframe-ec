package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dataframe.ErrorReporter;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.FunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public abstract class IntrinsicFunctionDescriptor
implements FunctionDescriptor
{
    private final String name;
    private final ListIterable<String> parameterNames;

    public IntrinsicFunctionDescriptor(String newName, ListIterable<String> newParameterNames)
    {
        this.name = newName;
        this.parameterNames = newParameterNames;
    }

    public IntrinsicFunctionDescriptor(String newName)
    {
        this(newName, Lists.immutable.of());
    }

    public IntrinsicFunctionDescriptor(String newName, String... parameterNames)
    {
        this(newName, Lists.immutable.of(parameterNames));
    }

    public String getName()
    {
        return this.name;
    }

    public ListIterable<String> getParameterNames()
    {
        return this.parameterNames;
    }

    public boolean hasExplicitParameters()
    {
        return this.parameterNames.size() > 0;
    }

    abstract public Value evaluate(EvalContext context);

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
        ErrorReporter.reportAndThrow(
                expectedCount != actualCount,
                "Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
    }
}
