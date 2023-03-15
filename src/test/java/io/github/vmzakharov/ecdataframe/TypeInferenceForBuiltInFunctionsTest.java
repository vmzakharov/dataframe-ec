package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.function.BuiltInFunctions;
import io.github.vmzakharov.ecdataframe.dsl.function.IntrinsicFunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.TypeInferenceUtil.assertScriptType;

public class TypeInferenceForBuiltInFunctionsTest
{
    @Test
    public void abs()
    {
        assertScriptType("abs(5)", ValueType.LONG);
        assertScriptType("abs(1.23)", ValueType.DOUBLE);
    }

    @Test
    public void startswith()
    {
        assertScriptType("startsWith('ABCD', 'AB')", ValueType.BOOLEAN);
    }

    @Test
    public void contains()
    {
        assertScriptType("contains('ABCD', 'AB')", ValueType.BOOLEAN);
    }

    @Test
    public void toUpper()
    {
        assertScriptType("toUpper('ABCD')", ValueType.STRING);
    }

    @Test
    public void substr()
    {
        assertScriptType("substr('ABCD', 2)", ValueType.STRING);
        assertScriptType("substr('ABCD', 1, 2)", ValueType.STRING);
    }

    @Test
    public void toStringFunction()
    {
        assertScriptType("toString('ABCD')", ValueType.STRING);
        assertScriptType("toString(123)", ValueType.STRING);
        assertScriptType("toString(12.34)", ValueType.STRING);
        assertScriptType("toString(toDate(2020, 10, 25))", ValueType.STRING);
    }

    @Test
    public void toLong()
    {
        assertScriptType("toLong('123')", ValueType.LONG);
    }

    @Test
    public void toDobule()
    {
        assertScriptType("toDouble('456')", ValueType.DOUBLE);
    }

    @Test
    public void toDate()
    {
        assertScriptType("toDate(2001, 11, 22)", ValueType.DATE);
    }

    @Test
    public void toDateTime()
    {
        assertScriptType("toDateTime(2001, 11, 22, 10, 10, 10)", ValueType.DATE_TIME);
    }

    @Test
    public void functionAddedAtRuntime()
    {
        BuiltInFunctions.addFunctionDescriptor(new IntrinsicFunctionDescriptor("two")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                return new LongValue(2);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return ValueType.LONG;
            }
        });

        assertScriptType("two()", ValueType.LONG);
    }
}
