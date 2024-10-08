package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.AssignExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.DecimalExpr;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionCallExpr;
import io.github.vmzakharov.ecdataframe.dsl.FunctionScript;
import io.github.vmzakharov.ecdataframe.dsl.IfElseExpr;
import io.github.vmzakharov.ecdataframe.dsl.IndexExpr;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.PropertyPathExpr;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.StatementSequenceScript;
import io.github.vmzakharov.ecdataframe.dsl.UnaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.VarExpr;
import io.github.vmzakharov.ecdataframe.dsl.VectorExpr;
import io.github.vmzakharov.ecdataframe.dsl.function.BuiltInFunctions;
import io.github.vmzakharov.ecdataframe.dsl.function.IntrinsicFunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DataFrameValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import org.eclipse.collections.api.list.ListIterable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class InMemoryEvaluationVisitor
implements ExpressionEvaluationVisitor
{
    final private EvalContext context;

    public InMemoryEvaluationVisitor()
    {
        this(new SimpleEvalContext());
    }

    public InMemoryEvaluationVisitor(EvalContext newContext)
    {
        this.context = newContext;
    }

    @Override
    public Value visitAssignExpr(AssignExpr expr)
    {
        return this.getContext().setVariable(expr.varName(), expr.expression().evaluate(this));
    }

    @Override
    public Value visitBinaryExpr(BinaryExpr expr)
    {
        return expr.operation().apply(
                () -> expr.operand1().evaluate(this),
                () -> expr.operand2().evaluate(this));
    }

    @Override
    public Value visitUnaryExpr(UnaryExpr expr)
    {
        return expr.operation().apply(expr.operand().evaluate(this));
    }

    @Override
    public Value visitConstExpr(Value expr)
    {
        return expr;
    }

    @Override
    public Value visitFunctionCallExpr(FunctionCallExpr expr)
    {
        String functionName = expr.normalizedFunctionName();
        SimpleEvalContext localContext = new SimpleEvalContext();
        ListIterable<Value> parameterValues = expr.parameters().collectWith(Expression::evaluate, this);

        FunctionScript functionScript = this.getContext().getDeclaredFunction(functionName);

        if (functionScript != null)
        {
            if (expr.parameters().size() != functionScript.getParameterNames().size())
            {
                exceptionByKey("DSL_EXPLICIT_PARAM_MISMATCH").with("functionName", functionName).fire();
            }

            functionScript.getParameterNames().forEachWithIndex(
                    (p, i) -> localContext.setVariable(p, parameterValues.get(i)));

            localContext.loadFunctionsExcept(this.context.getDeclaredFunctions(), functionName); // no recursion for you!

            return this.applyVisitorToScript(new InMemoryEvaluationVisitor(localContext), functionScript);
        }
        else
        {
            IntrinsicFunctionDescriptor functionDescriptor = BuiltInFunctions.getFunctionDescriptor(functionName);

            if (functionDescriptor == null)
            {
                exceptionByKey("DSL_UNKNOWN_FUN").with("functionName", expr.functionName()).fire();
            }

            if (functionDescriptor.hasExplicitParameters())
            {
                if (expr.parameters().size() != functionDescriptor.getParameterNames().size())
                {
                    exceptionByKey("DSL_EXPLICIT_PARAM_MISMATCH").with("functionName", functionName).fire();
                }

                functionDescriptor.getParameterNames().forEachWithIndex(
                        (p, i) -> localContext.setVariable(p, parameterValues.get(i)));
            }
            else
            {
                localContext.setVariable(functionDescriptor.magicalParameterName(), new VectorValue(parameterValues));
            }

            return functionDescriptor.evaluate(localContext);
        }
    }

    @Override
    public Value visitPropertyPathExpr(PropertyPathExpr expr)
    {
        HierarchicalDataSet dataSet = this.getContext().getDataSet(expr.getEntityName());
        Object rawValue = dataSet.getValue(expr.getPropertyChainString());

        if (rawValue == null)
        {
            return Value.VOID;
        }

        if (rawValue instanceof String s)
        {
            return new StringValue(s);
        }

        if (rawValue instanceof Integer i)
        {
            return new IntValue(i);
        }

        if (rawValue instanceof Long l)
        {
            return new LongValue(l);
        }

        if (rawValue instanceof Float f)
        {
            return new FloatValue(f);
        }

        if (rawValue instanceof Double d)
        {
            return new DoubleValue(d);
        }

        if (rawValue instanceof LocalDate ld)
        {
            return new DateValue(ld);
        }

        if (rawValue instanceof LocalDateTime ldt)
        {
            return new DateTimeValue(ldt);
        }

        if (rawValue instanceof BigDecimal bd)
        {
            return new DecimalValue(bd);
        }

        throw exceptionByKey("DSL_FAIL_TO_CONVERT_RAW_VAL")
                .with("rawValue", rawValue)
                .with("rawValueType", rawValue.getClass().getName())
                .get();
    }

    @Override
    public Value visitAnonymousScriptExpr(AnonymousScript script)
    {
        this.getContext().setDeclaredFunctions(script.getFunctions());
        return this.applyVisitorToScript(this, script);
    }

    private Value applyVisitorToScript(ExpressionEvaluationVisitor evaluationVisitor, Script script)
    {
        Value result = Value.VOID;
        int expressionCount = script.getExpressions().size();
        for (int i = 0; i < expressionCount; i++)
        {
            Expression expression = script.getExpressions().get(i);
            result = expression.evaluate(evaluationVisitor);
        }

        return result;
    }

    @Override
    public Value visitFunctionScriptExpr(FunctionScript expr)
    {
        throw exceptionByKey("DSL_FUN_DECLARATION_EVAL").with("functionName", expr.getName()).get();
    }

    @Override
    public Value visitIfElseExpr(IfElseExpr expr)
    {
        BooleanValue condValue = (BooleanValue) expr.condition().evaluate(this);
        if (condValue.isTrue())
        {
            return expr.ifScript().evaluate(this);
        }

        if (expr.hasElseSection())
        {
            return expr.elseScript().evaluate(this);
        }

        return Value.VOID;
    }

    @Override
    public Value visitStatementSequenceScriptExpr(StatementSequenceScript expr)
    {
        return this.applyVisitorToScript(this, expr);
    }

    @Override
    public Value visitVarExpr(VarExpr expr)
    {
        return this.getContext().getVariable(expr.variableName());
    }

    @Override
    public Value visitProjectionExpr(ProjectionExpr expr)
    {
        PropertyPathExpr propertyPathExpr = (PropertyPathExpr) expr.projectionExpressions().detect(
                e -> e instanceof PropertyPathExpr);

        HierarchicalDataSet dataSet = this.getContext().getDataSet(propertyPathExpr.getEntityName());

        boolean missingWhereClause = expr.whereClause() == null;

        DataFrame dataFrame = new DataFrame("Projection");

        TypeInferenceVisitor typeInferenceVisitor = new TypeInferenceVisitor(this.getContext());
        ListIterable<ValueType> projectionExpressionTypes = expr.projectionExpressions().collect(typeInferenceVisitor::inferExpressionType);

        expr.elementNames().zip(projectionExpressionTypes).each(p -> dataFrame.addColumn(p.getOne(), p.getTwo()));

        dataSet.openFileForReading();
        while (dataSet.hasNext())
        {
            dataSet.next();
            if (missingWhereClause || ((BooleanValue) expr.whereClause().evaluate(this)).isTrue())
            {
                ListIterable<Value> rowValues = expr.projectionExpressions().collect(e -> e.evaluate(this));
                dataFrame.addRow(rowValues);
            }
        }
        dataSet.close();

        return new DataFrameValue(dataFrame);
    }

    @Override
    public Value visitVectorExpr(VectorExpr expr)
    {
        return new VectorValue(
                expr.elements().collect(e -> e.evaluate(this))
        );
    }

    @Override
    public Value visitIndexExpr(IndexExpr expr)
    {
        VectorValue vector = (VectorValue) expr.vectorExpr().evaluate(this);
        LongValue index = (LongValue) expr.indexExpr().evaluate(this);
        return vector.get((int) index.longValue());
    }

    @Override
    public Value visitDecimalExpr(DecimalExpr expr)
    {
        LongValue unscaledValue = (LongValue) expr.unscaledValueExpr().evaluate(this);
        LongValue scale = (LongValue) expr.scaleExpr().evaluate(this);
        return new DecimalValue(unscaledValue.longValue(), (int) scale.longValue());
    }

    public EvalContext getContext()
    {
        return this.context;
    }
}
