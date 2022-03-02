package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.PrettyPrintVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.Assert;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.TypeInferenceUtil.assertScriptType;
import static io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor.ERR_IF_ELSE_INCOMPATIBLE;
import static io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor.ERR_TYPES_IN_EXPRESSION;
import static io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor.ERR_UNDEFINED_VARIABLE;

public class TypeInferenceTest
{
    @Test
    public void intTypeInference()
    {
        assertScriptType("(1 + 2) * 3", ValueType.LONG);
        assertScriptType("x = (1 + 2) * 3", ValueType.LONG);
        assertScriptType("-123", ValueType.LONG);
    }

    @Test
    public void doubleTypeInference()
    {
        assertScriptType("(1.0 + 2.0) * 3.0", ValueType.DOUBLE);
        assertScriptType("x = 1.0 + 2.0", ValueType.DOUBLE);
        assertScriptType("x = (1 + 2) * 3.0", ValueType.DOUBLE);
        assertScriptType("-4.56", ValueType.DOUBLE);
    }

    @Test
    public void stringTypeInference()
    {
        assertScriptType("\"abc\"", ValueType.STRING);
        assertScriptType("x = \"abc\" + \"def\"", ValueType.STRING);
    }

    @Test
    public void booleanTypeInference()
    {
        assertScriptType("'abc' == 'xyz'", ValueType.BOOLEAN);
        assertScriptType("1 in (1, 2, 3)", ValueType.BOOLEAN);
        assertScriptType("(1, 2, 3) is empty", ValueType.BOOLEAN);
        assertScriptType("'' is not empty", ValueType.BOOLEAN);
        assertScriptType("not ((5 > 6) or (1 in (1, 2, 3)))", ValueType.BOOLEAN);
    }

    @Test
    public void isNullInference()
    {
        assertScriptType("(1, 2, 3) is null", ValueType.BOOLEAN);
        assertScriptType("'' is not null", ValueType.BOOLEAN);
        assertScriptType("123 is null", ValueType.BOOLEAN);
        assertScriptType("123 is not null", ValueType.BOOLEAN);
        assertScriptType("1.234 is null", ValueType.BOOLEAN);
        assertScriptType("toDate(2020, 12, 20) is null", ValueType.BOOLEAN);
        assertScriptType("toDate(2020, 12, 20) is not null", ValueType.BOOLEAN);
        assertScriptType(
                  "a = 1\n"
                + "if a is null then\n"
                + "  'null'\n"
                + "else\n"
                + "  'not null'\n"
                + "endif", ValueType.STRING);
    }

    @Test
    public void scriptTypeInference()
    {
        assertScriptType(
                   "1\n"
                + "2.0\n"
                + "\"abc\"", ValueType.STRING);

        assertScriptType(
                  "a = 1\n"
                + "b = 2.0\n"
                + "c = a + b", ValueType.DOUBLE);
    }

    @Test
    public void functionTypeInference()
    {
        assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1.0\n"
                + "y = 2.0\n"
                + "sum(x, y)", ValueType.DOUBLE);

        assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1\n"
                + "y = 2\n"
                + "sum(x, y)", ValueType.LONG);

        assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = \"Hello, \"\n"
                + "y = \"world!\"\n"
                + "sum(x, y)", ValueType.STRING);

        assertScriptType(
                  "function sum(a, b)\n"
                + "{\n"
                + "    a + b\n"
                + "}\n"
                + "x = 1\n"
                + "y = 2.0\n"
                + "sum(x, y)", ValueType.DOUBLE);

        assertScriptType(
                  "function isItBigger(a, b) { a > b }\n"
                + "x = 1\n"
                + "y = 2.0\n"
                + "isItBigger(x, y)", ValueType.BOOLEAN);
    }

    @Test
    public void conditionalStatement()
    {
        assertScriptType(" a > b ? 5 : 7", ValueType.LONG);
        assertScriptType(" a > b ? 5.0 : 7", ValueType.DOUBLE);
        assertScriptType(" a > b ? 'foo' : 'bar'", ValueType.STRING);
        assertScriptType(" a > b ? 'foo' : 7", ValueType.VOID);
    }

    @Test
    public void conditionalOnlyIfBranch()
    {
        assertScriptType(" if a > b then\n  5\nendif", ValueType.LONG);
        assertScriptType(" if a > b then\n  5.5 + 1\nendif", ValueType.DOUBLE);
        assertScriptType(" if a > b then\n  '5'\nendif", ValueType.STRING);
    }

    @Test
    public void withContext()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", new LongValue(7));
        assertScriptType("a + b", context, ValueType.LONG);
    }

    @Test
    public void containsIncompatibleTypes()
    {
        this.assertError("1 in 'abc'", this.prettyPrint("1 in 'abc'"), ERR_TYPES_IN_EXPRESSION);
        this.assertError("1.1 not in 'abc'", this.prettyPrint("1.1 not in 'abc'"), ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void expressionIncompatibleTypes()
    {
        this.assertError(
                "x = 5\ny = 'abc'\nx + y",
                "(x + y)",
                ERR_TYPES_IN_EXPRESSION);
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
                this.prettyPrint("if a > b then\n"
                        + "  x\n"
                        + "else\n"
                        + "  'abc'\n"
                        + "endif"),
                ERR_IF_ELSE_INCOMPATIBLE);
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
                this.prettyPrint("if a > b then\n  x\nelse\n y\nendif"),
                ERR_IF_ELSE_INCOMPATIBLE);
    }

    @Test
    public void comparisonIncompatibleTypes()
    {
        this.assertError(
                "x = 5\nx == 'abc'\n",
                this.prettyPrint("x == 'abc'"),
                ERR_TYPES_IN_EXPRESSION);
    }

    /*
     * note that the "cascading" errors are ignored, i.e. we won't generate an error for
     * x + 1 if x is undefined if we have already generated an error for x being undefined
     */
    @Test
    public void manyErrorsInOneScriptCatchesAllErrors()
    {
        this.assertErrors(
                  "x = 5\n"
                + "x + 'abc'\n"
                + "'x' < 1\n",
                Lists.immutable.of(
                        Tuples.twin(ERR_TYPES_IN_EXPRESSION, "(x + \"abc\")"),
                        Tuples.twin(ERR_TYPES_IN_EXPRESSION, "(\"x\" < 1)")
                ));

        this.assertErrors(
                  "y = 5\n"
                + "x + 1\n"
                + "'x' < y\n",
                Lists.immutable.of(
                        Tuples.twin(ERR_UNDEFINED_VARIABLE, "x"),
                        Tuples.twin(ERR_TYPES_IN_EXPRESSION, "(\"x\" < y)")
                ));

        this.assertErrors(
                  "if x > 3\n"
                + "then 'abc'\n"
                + "else 1\n"
                + "endif",
                Lists.immutable.of(
                        Tuples.twin(ERR_UNDEFINED_VARIABLE, "x"),
                        Tuples.twin(ERR_IF_ELSE_INCOMPATIBLE, "if (x > 3) then\n"
                                + "  \"abc\"\n"
                                + "else\n"
                                + "  1\n"
                                + "endif")
                ));
    }

    @Test
    public void comparisonIncompatibleTypesWithContextVariables()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new LongValue(1));
        this.assertError(context,
                "x == 'abc'\n",
                this.prettyPrint("x == 'abc'"),
                ERR_TYPES_IN_EXPRESSION);
    }

    @Test
    public void undefinedVariables()
    {
        this.assertError("1 + abc", "abc", ERR_UNDEFINED_VARIABLE);
        this.assertError("x == 'abc'\ny + x", "x", ERR_UNDEFINED_VARIABLE);
        this.assertError(
                  "if x == 4\n"
                + "then 'four'\n"
                + "else 'not four'\n"
                + "endif",
                "x", ERR_UNDEFINED_VARIABLE);
    }

    private void assertErrors(String scriptString, ListIterable<Twin<String>> expectedErrors)
    {
        Script script = ExpressionTestUtil.toScript(scriptString);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor();
        script.accept(visitor);

        Assert.assertTrue("Expected a type inference error", visitor.hasErrors());

        Assert.assertEquals(expectedErrors, visitor.getErrors());
    }

    private void assertError(String scriptString, String errorDescription, String errorText)
    {
        this.assertError(new SimpleEvalContext(), scriptString, errorDescription, errorText);
    }

    private void assertError(EvalContext context, String scriptString, String errorDescription, String errorText)
    {
        Script script = ExpressionTestUtil.toScript(scriptString);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);
        script.accept(visitor);

        Assert.assertTrue("Expected a type inference error", visitor.hasErrors());
        Assert.assertEquals("Error type", errorText, visitor.getErrorDescription());

        Assert.assertEquals("Error expression", errorDescription, visitor.getErrorExpressionString());
    }
    
    private String prettyPrint(String expressionString)
    {
        return PrettyPrintVisitor.exprToString(
                ExpressionTestUtil.toScript(expressionString).getExpressions().get(0));
    }
}
