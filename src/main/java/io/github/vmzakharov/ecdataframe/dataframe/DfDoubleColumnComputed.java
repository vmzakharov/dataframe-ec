package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
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
    protected void addAllItems(DoubleIterable items)
    {
        this.throwUnmodifiableColumnException();
    }
}
