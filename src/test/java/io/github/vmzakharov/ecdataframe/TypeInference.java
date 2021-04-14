package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import org.junit.Assert;
import org.junit.Test;

public class TypeInference
{
    @Test
    public void intTypeInference()
    {
        this.assertExpressionType("(1 + 2) * 3", ValueType.LONG);
        this.assertExpressionType("x = (1 + 2) * 3", ValueType.LONG);
    }

    @Test
    public void doubleTypeInference()
    {
        this.assertExpressionType("(1.0 + 2.0) * 3.0", ValueType.DOUBLE);
        this.assertExpressionType("x = 1.0 + 2.0", ValueType.DOUBLE);
        this.assertExpressionType("x = (1 + 2) * 3.0", ValueType.DOUBLE);
    }

    @Test
    public void stringTypeInference()
    {
        this.assertExpressionType("\"abc\"", ValueType.STRING);
        this.assertExpressionType("x = \"abc\" + \"def\"", ValueType.STRING);
    }

    @Test
    public void scriptTypeInference()
    {
        this.assertScriptType(
                   "1\n"
                + "2.0\n"
                + "\"abc\"", ValueType.STRING);

        this.assertScriptType(
                  "a = 1\n"
                + "b = 2.0\n"
                + "c = a + b", ValueType.DOUBLE);
    }

    @Test
    public void functionTypeInference()
    {
        this.assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1.0\n"
                + "y = 2.0\n"
                + "sum(x, y)", ValueType.DOUBLE);

        this.assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1\n"
                + "y = 2\n"
                + "sum(x, y)", ValueType.LONG);

        this.assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = \"Hello, \"\n"
                + "y = \"world!\"\n"
                + "sum(x, y)", ValueType.STRING);

        this.assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1\n"
                + "y = 2.0\n"
                + "sum(x, y)", ValueType.DOUBLE);

        this.assertScriptType(
                  "function isItBigger(a, b) { a > b }\n"
                + "x = 1\n"
                + "y = 2.0\n"
                + "isItBigger(x, y)", ValueType.BOOLEAN);
    }

    private void assertScriptType(String scriptAsString, ValueType valueType)
    {
        this.assertType(ExpressionTestUtil.toScript(scriptAsString), valueType);
    }

    private void assertExpressionType(String expressionAsString, ValueType valueType)
    {
        this.assertType(ExpressionTestUtil.toExpression(expressionAsString), valueType);
    }

    private void assertType(Expression expression, ValueType valueType)
    {
        TypeInferenceVisitor visitor = new TypeInferenceVisitor();
        expression.accept(visitor);
        Assert.assertEquals(valueType, visitor.getLastExpressionType());
    }
}
