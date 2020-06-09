package org.modelscript.dataframe;

import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.impl.list.Interval;
import org.modelscript.expr.DataFrameEvalContext;
import org.modelscript.expr.Expression;
import org.modelscript.expr.value.DoubleValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class DfDoubleColumnComputed
extends DfDoubleColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfDoubleColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.toExpression(expressionAsString);
    }

    @Override
    public void addValue(Value value)
    {
        this.unmodifiableColumnException();
    }

    @Override
    public void addObject(Object newObject)
    {
        this.unmodifiableColumnException();
    }

    @Override
    public double getDouble(int rowIndex)
    {
        // todo: column in the variable expr or some other optimization?

        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        DoubleValue result = (DoubleValue) this.expression.evaluate(new InMemoryEvaluationVisitor(evalContext));
        return result.doubleValue();
    }

    @Override
    public ImmutableDoubleList toDoubleList()
    {
        ImmutableDoubleList result = Interval.zeroTo(this.getDataFrame().rowCount() - 1)
                .collectDouble(this::getDouble)
                .toList()
                .toImmutable();
        return result;
    }

    @Override
    public double sum()
    {
        return Interval.zeroTo(this.getDataFrame().rowCount() - 1)
                .asLazy()
                .collectDouble(this::getDouble)
                .sum();
    }

    @Override
    public int getSize()
    {
        return this.getDataFrame().rowCount();
    }

    @Override
    public String getExpressionAsString()
    {
        return this.expressionAsString;
    }
}
