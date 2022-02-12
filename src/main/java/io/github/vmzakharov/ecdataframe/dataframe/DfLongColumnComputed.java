package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.Interval;

public class DfLongColumnComputed
extends DfLongColumn
implements DfColumnComputed
{
    private final String expressionAsString;
    private final Expression expression;

    public DfLongColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName);
        this.expressionAsString = newExpressionAsString;
        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public long getLong(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        if (result.isVoid())
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return ((LongValue) result).longValue();
    }

    @Override
    public ImmutableLongList toLongList()
    {
        if (this.getDataFrame().rowCount() == 0)
        {
            return LongLists.immutable.empty();
        }

        ImmutableLongList result = Interval
                .zeroTo(this.getDataFrame().rowCount() - 1)
                .collectLong(this::getLong)
                .toList()
                .toImmutable();
        return result;
    }

    @Override
    public Object getObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((LongValue) result).longValue();
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    @Override
    public Number aggregate(AggregateFunction aggregateFunction)
    {
        if (this.getSize() == 0)
        {
            return aggregateFunction.defaultLongIfEmpty();
        }

        return aggregateFunction.applyLongIterable(
                Interval.zeroTo(this.getDataFrame().rowCount() - 1).collectLong(this::getLong)
        );
    }

    @Override
    protected void addAllItemsFrom(DfLongColumn longColumn)
    {
        this.throwUnmodifiableColumnException();
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
}
