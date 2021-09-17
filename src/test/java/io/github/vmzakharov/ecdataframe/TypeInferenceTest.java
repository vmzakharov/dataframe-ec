package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.PrettyPrintVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import org.junit.Assert;
import org.junit.Test;

public class TypeInferenceTest
{
    @Test
    public void intTypeInference()
    {
        this.assertExpressionType("(1 + 2) * 3", ValueType.LONG);
        this.assertExpressionType("x = (1 + 2) * 3", ValueType.LONG);
        this.assertExpressionType("-123", ValueType.LONG);
    }

    @Test
    public void doubleTypeInference()
    {
        this.assertExpressionType("(1.0 + 2.0) * 3.0", ValueType.DOUBLE);
        this.assertExpressionType("x = 1.0 + 2.0", ValueType.DOUBLE);
        this.assertExpressionType("x = (1 + 2) * 3.0", ValueType.DOUBLE);
        this.assertExpressionType("-4.56", ValueType.DOUBLE);
    }

    @Test
    public void stringTypeInference()
    {
        this.assertExpressionType("\"abc\"", ValueType.STRING);
        this.assertExpressionType("x = \"abc\" + \"def\"", ValueType.STRING);
    }

    @Test
    public void booleanTypeInference()
    {
        this.assertExpressionType("'abc' == 'xyz'", ValueType.BOOLEAN);
        this.assertExpressionType("1 in (1, 2, 3)", ValueType.BOOLEAN);
        this.assertExpressionType("(1, 2, 3) is empty", ValueType.BOOLEAN);
        this.assertExpressionType("'' is not empty", ValueType.BOOLEAN);
        this.assertExpressionType("not ((5 > 6) or (1 in (1, 2, 3)))", ValueType.BOOLEAN);
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

    @Test
    public void conditionalStatement()
    {
        this.assertExpressionType(" a > b ? 5 : 7", ValueType.LONG);
        this.assertExpressionType(" a > b ? 5.0 : 7", ValueType.DOUBLE);
        this.assertExpressionType(" a > b ? 'foo' : 'bar'", ValueType.STRING);
        this.assertExpressionType(" a > b ? 'foo' : 7", ValueType.VOID);
    }

    @Test
    public void conditionalOnlyIfBranch()
    {
        this.assertScriptType(" if a > b then\n  5\nendif", ValueType.LONG);
        this.assertScriptType(" if a > b then\n  5.5 + 1\nendif", ValueType.DOUBLE);
        this.assertScriptType(" if a > b then\n  '5'\nendif", ValueType.STRING);
    }

    @Test
    public void containsIncompatibleTypes()
    {
        this.assertError("1 in 'abc'", 0, TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);
        this.assertError("1.1 not in 'abc'", 0, TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void expressionIncompatibleTypes()
    {
        this.assertError(
                "x = 5\ny = 'abc'\nx + y",
                2,
                TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void conditionalIncompatibleTypes()
    {
        this.assertError(
                   "a = 1\n"
                + "b = 2\n"
                + "x = 5\n"
                + "if a > b then\n"
                + "  x\n"
                + "else\n"
                + "  'abc'\n"
                + "endif",
                3,
                TypeInferenceVisitor.ERR_IF_ELSE_INCOMPATIBLE);
    }

    @Test
    public void conditionalIncompatibleTypesWithContextVariables()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", new LongValue(7));
        context.setVariable("y", new StringValue("abc"));
        this.assertError(context,
                "x = 5\nif a > b then\n  x\nelse\n y\nendif",
                1,
                TypeInferenceVisitor.ERR_IF_ELSE_INCOMPATIBLE);
    }

    @Test
    public void comparisonIncompatibleTypes()
    {
        this.assertError(
                "x = 5\nx == 'abc'\n",
                1,
                TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void manyErrorsInOneScriptCatchesTheFirstError()
    {
        this.assertError(
                  "x = 5\n"
                + "x + 'abc'\n"
                + "'x' < 1\n",
                1,
                TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);

        this.assertError(
                  "if x > 3\n"
                + "then 'abc'\n"
                + "else 1\n"
                + "endif",
                "x",
                TypeInferenceVisitor.ERR_UNDEFINED_VARIABLE);
    }

    @Test
    public void comparisonIncompatibleTypesWithContextVariables()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new LongValue(1));
        this.assertError(context,
                "x == 'abc'\n",
                0,
                TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void undefinedVariables()
    {
        this.assertError("1 + abc", "abc", TypeInferenceVisitor.ERR_UNDEFINED_VARIABLE);
        this.assertError("x == 'abc'\ny + x", "x", TypeInferenceVisitor.ERR_UNDEFINED_VARIABLE);
        this.assertError(
                  "if x == 4\n"
                + "then 'four'\n"
                + "else 'not four'\n"
                + "endif",
                "x", TypeInferenceVisitor.ERR_UNDEFINED_VARIABLE);
    }

    private void assertError(String scriptString, int errorExpressionIndex, String errorText)
    {
        this.assertError(new SimpleEvalContext(), scriptString, errorExpressionIndex, errorText);
    }

    private void assertError(String scriptString, String errorDescription, String errorText)
    {
        this.assertError(new SimpleEvalContext(), scriptString, errorDescription, errorText);
    }

    private void assertError(EvalContext context, String scriptString, int errorExpressionIndex, String errorText)
    {
        this.assertError(context, scriptString, errorExpressionIndex, null, errorText);
    }

    private void assertError(EvalContext context, String scriptString, String errorDescription, String errorText)
    {
        this.assertError(context, scriptString, -1, errorDescription, errorText);
    }

    private void assertError(EvalContext context, String scriptString,  int errorExpressionIndex, String errorDescription, String errorText)
    {
        Script script = ExpressionTestUtil.toScript(scriptString);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);
        script.accept(visitor);

        Assert.assertTrue("Expected a type inference error", visitor.hasErrors());
        Assert.assertEquals("Error type", errorText, visitor.getErrorDescription());

        String expectedErrorExpression =
                errorExpressionIndex >= 0
                        ? PrettyPrintVisitor.exprToString(script.getExpressions().get(errorExpressionIndex))
                        : errorDescription;

        Assert.assertEquals("Error expression", expectedErrorExpression, visitor.getErrorExpressionString());
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
        this.assertType(new SimpleEvalContext(), expression, valueType);
    }

    private void assertType(EvalContext context, Expression expression, ValueType valueType)
    {
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);
        expression.accept(visitor);
        Assert.assertEquals(valueType, visitor.getLastExpressionType());
    }
}
