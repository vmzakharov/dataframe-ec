package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.Interval;

abstract public class DfObjectColumnComputed<T>
extends DfObjectColumn<T>
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfObjectColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public ImmutableList<T> toList()
    {
        if (this.getDataFrame().rowCount() == 0)
        {
            return Lists.immutable.empty();
        }

        ImmutableList<T> result = Interval
                .zeroTo(this.getDataFrame().rowCount() - 1)
                .collect(this::getTypedObject)
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

    protected Expression getExpression()
    {
        return this.expression;
    }

    @Override
    protected void addAllItems(ListIterable<T> items)
    {
        this.throwUnmodifiableColumnException();
    }
}
