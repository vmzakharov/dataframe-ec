package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;

import java.util.function.Function;

public class IntrinsicFunctionDescriptorBuilder
extends IntrinsicFunctionDescriptor
{
    private Function<EvalContext, Value> action = ignore -> Value.VOID;
    private Function<ListIterable<ValueType>, ValueType> returnTypeFunction = ignore -> ValueType.VOID;

    public IntrinsicFunctionDescriptorBuilder(
            String newName,
            ListIterable<String> newParameterNames)
    {
        super(newName, newParameterNames);
    }

    public IntrinsicFunctionDescriptorBuilder action(Function<EvalContext, Value> newAction)
    {
        this.action = newAction;
        return this;
    }

    public IntrinsicFunctionDescriptorBuilder returnType(ValueType newReturnType)
    {
        this.returnType(ignore -> newReturnType);
        return this;
    }

    public IntrinsicFunctionDescriptorBuilder returnType(Function<ListIterable<ValueType>, ValueType> newReturnTypeFunction)
    {
        this.returnTypeFunction = newReturnTypeFunction;
        return this;
    }

    @Override
    public Value evaluate(EvalContext context)
    {
        return this.action.apply(context);
    }

    @Override
    public ValueType returnType(ListIterable<ValueType> parameterTypes)
    {
        return this.returnTypeFunction.apply(parameterTypes);
    }
}
