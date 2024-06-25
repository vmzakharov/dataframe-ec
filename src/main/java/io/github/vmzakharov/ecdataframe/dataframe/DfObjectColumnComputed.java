package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

abstract public class DfObjectColumnComputed<T>
extends DfObjectColumnAbstract<T>
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
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return Lists.immutable.empty();
        }

        return IntInterval
                .zeroTo(rowCount - 1)
                .collect(this::getTypedObject, Lists.mutable.withInitialCapacity(rowCount))
                .toImmutable();
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
    public Expression getExpression()
    {
        return this.expression;
    }

    @Override
    protected void addAllItems(ListIterable<T> items)
    {
        this.throwUnmodifiableColumnException();
    }
}
