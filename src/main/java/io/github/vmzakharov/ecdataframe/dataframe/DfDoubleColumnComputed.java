package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.list.Interval;

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

        this.expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(this.expressionAsString);
    }

    @Override
    public Object getObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((DoubleValue) result).doubleValue();
    }

    @Override
    public double getDouble(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        if (result.isVoid())
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return ((DoubleValue) result).doubleValue();
    }

    @Override
    public ImmutableDoubleList toDoubleList()
    {
        if (this.getDataFrame().rowCount() == 0)
        {
            return DoubleLists.immutable.empty();
        }

        ImmutableDoubleList result = Interval.zeroTo(this.getDataFrame().rowCount() - 1)
                .collectDouble(this::getDouble)
                .toList()
                .toImmutable();
        return result;
    }

    @Override
    public Number aggregate(AggregateFunction aggregateFunction)
    {
        if (this.getSize() == 0)
        {
            return aggregateFunction.defaultDoubleIfEmpty();
        }

        return aggregateFunction.applyDoubleIterable(
                Interval.zeroTo(this.getDataFrame().rowCount() - 1).collectDouble(this::getDouble)
        );
    }

    @Override
    protected void addAllItemsFrom(DfDoubleColumn doubleColumn)
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

    @Override
    protected void addAllItems(DoubleIterable items)
    {
        this.throwUnmodifiableColumnException();
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }
}
