package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

public class RuntimeAddedFunctionTest
{
    @Test
    public void simple()
    {
        BuiltInFunctions.addFunctionDescriptor(new IntrinsicFunctionDescriptor("plusTwo", Lists.immutable.of("number"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                return new LongValue(
                        ((LongValue) context.getVariable("number")).longValue() + 2
                );
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return ValueType.LONG;
            }
        });

        Assert.assertEquals(5, ExpressionTestUtil.evaluateToLong("plusTwo(3)"));
        Assert.assertEquals(4, ExpressionTestUtil.evaluateToLong("abs(plusTwo(-6))"));
    }
}
