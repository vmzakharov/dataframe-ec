package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.impl.list.Interval;
import org.modelscript.expr.DataFrameEvalContext;
import org.modelscript.expr.Expression;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class DfStringColumnComputed
extends DfStringColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfStringColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
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
    public String getString(int rowIndex)
    {
        // todo: column in the variable expr or some other optimization?

        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        StringValue result = (StringValue) this.expression.evaluate(new InMemoryEvaluationVisitor(evalContext));
        return result.stringValue();
    }

    @Override
    public ImmutableList<String> toList()
    {
        ImmutableList<String> result = Interval
                .zeroTo(this.getDataFrame().rowCount() - 1)
                .collect(this::getString)
                .toList()
                .toImmutable();
        return result;
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
