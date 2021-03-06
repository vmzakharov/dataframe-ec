package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.Interval;

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
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
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
        if (this.getDataFrame().rowCount() == 0)
        {
            return Lists.immutable.empty();
        }

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

    @Override
    protected void addAllItems(ListIterable<String> items)
    {
        this.throwUnmodifiableColumnException();
    }
}
