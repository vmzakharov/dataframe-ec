package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataset.AvroDataSet;
import io.github.vmzakharov.ecdataframe.dsl.*;
import io.github.vmzakharov.ecdataframe.dsl.function.BuiltInFunctions;
import io.github.vmzakharov.ecdataframe.dsl.function.IntrinsicFunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.*;
import org.eclipse.collections.api.list.ListIterable;

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
    public Value visitAssignExpr(AssingExpr expr)
    {
        return this.getContext().  setVariable(expr.getVarName(), expr.getExpression().evaluate(this));
    }

    @Override
    public Value visitBinaryExpr(BinaryExpr expr)
    {
        return expr.getOperation().apply(
                expr.getOperand1().evaluate(this),
                expr.getOperand2().evaluate(this));
    }

    @Override
    public Value visitUnaryExpr(UnaryExpr expr)
    {
        return expr.getOperation().apply(expr.getOperand().evaluate(this));
    }

    @Override
    public Value visitConstExpr(ConstExpr expr)
    {
        return expr.getValue();
    }

    @Override
    public Value visitFunctionCallExpr(FunctionCallExpr expr)
    {
        String functionName = expr.getFunctionName();
        SimpleEvalContext localContext = new SimpleEvalContext();
        ListIterable<Value> parameterValues = expr.getParameters().collectWith(Expression::evaluate, this);

        FunctionScript functionScript = this.getContext().getDeclaredFunction(functionName);

        if (functionScript != null)
        {
            if (expr.getParameters().size() != functionScript.getParameterNames().size())
            {
                throw new RuntimeException("Parameter count mismatch in an invocation of '" + functionName + "'");
            }

            functionScript.getParameterNames().forEachWithIndex(
                    (p, i) -> localContext.setVariable(p, parameterValues.get(i)));

            localContext.loadFunctionsExcept(context.getDeclaredFunctions(), functionName); // no recursion for you!

            return this.applyVisitorToScript(new InMemoryEvaluationVisitor(localContext), functionScript);
        }
        else
        {
            IntrinsicFunctionDescriptor functionDescriptor = BuiltInFunctions.getFunctionDescriptor(functionName);

            if (functionDescriptor == null)
            {
                throw new RuntimeException("Unknown function: '" + expr.getFunctionName() + "'");
            }

            if (functionDescriptor.hasExplicitParameters())
            {
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
        AvroDataSet dataSet = this.getContext().getDataSet(expr.getEntityName());
        Object rawValue = dataSet.getValue(expr.getPropertyChainString());

        if (rawValue == null)
        {
            return Value.VOID;
        }

        if (rawValue instanceof String || rawValue instanceof org.apache.avro.util.Utf8)
        {
            return new StringValue(rawValue.toString());
        }

        if (rawValue instanceof Integer)
        {
            return new LongValue((Integer) rawValue);
        }

        throw new RuntimeException("Don't know how to handle " + rawValue.toString() + ", " + rawValue.getClass().getName());
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
        throw new RuntimeException("Cannot evaluate function declaration by itself. Function " + expr.getName());
    }

    @Override
    public Value visitIfElseExpr(IfElseExpr expr)
    {
        BooleanValue condValue = (BooleanValue) expr.getCondition().evaluate(this);
        if (condValue.isTrue())
        {
            return expr.getIfScript().evaluate(this);
        }

        if (expr.hasElseSection())
        {
            return expr.getElseScript().evaluate(this);
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
        return this.getContext().getVariable(expr.getVariableName());
    }

    @Override
    public Value visitProjectionExpr(ProjectionExpr expr)
    {
        PropertyPathExpr propertyPathExpr = (PropertyPathExpr) expr.getProjectionExpressions().detect(
                e -> e instanceof PropertyPathExpr);

        AvroDataSet dataSet = this.getContext().getDataSet(propertyPathExpr.getEntityName());

        boolean missingWhereClause = ( expr.getWhereClause() == null );

        DataFrame dataFrame = new DataFrame("Projection");

        TypeInferenceVisitor typeInferenceVisitor = new TypeInferenceVisitor(this.getContext());
        ListIterable<ValueType> projectionExpressionTypes = expr.getProjectionExpressions().collect(typeInferenceVisitor::inferExpressionType);

        expr.getElementNames().zip(projectionExpressionTypes).each(p -> dataFrame.addColumn(p.getOne(), p.getTwo()));

        dataSet.openFileForReading();
        while (dataSet.hasNext())
        {
            dataSet.next();
            if (missingWhereClause || ((BooleanValue) expr.getWhereClause().evaluate(this)).isTrue())
            {
                ListIterable<Value> rowValues = expr.getProjectionExpressions().collect(e -> e.evaluate(this));
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
                expr.getElements().collect(e -> e.evaluate(this))
        );
    }

    @Override
    public Value visitIndexExpr(IndexExpr expr)
    {
        VectorValue vector = (VectorValue) expr.getVectorExpr().evaluate(this);
        LongValue index = (LongValue) expr.getIndexExpr().evaluate(this);
        return vector.get((int) index.longValue());
    }

    public EvalContext getContext()
    {
        return this.context;
    }
}
