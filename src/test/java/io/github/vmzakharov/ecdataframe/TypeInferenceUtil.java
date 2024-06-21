package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import org.junit.jupiter.api.Assertions;

final public class TypeInferenceUtil
{
    private TypeInferenceUtil()
    {
        // utility class
    }

    public static void assertScriptType(String scriptAsString, ValueType valueType)
    {
        assertScriptType(scriptAsString, new SimpleEvalContext(), valueType);
    }

    public static void assertScriptType(String scriptAsString, EvalContext context, ValueType valueType)
    {
        Expression expression = ExpressionTestUtil.toScript(scriptAsString);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);
        expression.accept(visitor);
        Assertions.assertEquals(valueType, visitor.getLastExpressionType());
    }
}
