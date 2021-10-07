package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import org.eclipse.collections.api.list.ListIterable;
import org.junit.Assert;
import org.junit.Test;

public class RuntimeAddedFuntionTest
{
    @Test
    public void simple()
    {
        BuiltInFunctions.addFunctionDescriptor(new IntrinsicFunctionDescriptor("plusTwo")
        {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                return new LongValue(
                        ((LongValue) parameters.get(0)).longValue() + 2);
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
