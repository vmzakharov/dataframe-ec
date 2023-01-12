package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public interface DfColumnComputed
extends DfColumn
{
    @Override
    default boolean isStored()
    {
        return false;
    }

    String getExpressionAsString();

    Expression getExpression();

    @Override
    default void setObject(int rowIndex, Object anObject)
    {
        exceptionByKey("DF_SET_VAL_ON_COMP_COL").with("columnName", this.getName()).fire();
    }

    @Override
    default Value getValue(int rowIndex)
    {
        // todo: column in the variable expr or some other optimization?
        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        return this.getExpression().evaluate(new InMemoryEvaluationVisitor(evalContext));
    }

    @Override
    default void addEmptyValue()
    {
        // no-op
    }

    @Override
    default void addValue(Value value)
    {
        this.throwUnmodifiableColumnException();
    }

    /**
     * a placeholder method for computed columns defined so that {@code addRow()} could be called on a data frame with
     * computed columns. If any value other than {@code null} is passed the method throws an exception
     * @param newObject an object to add to the column, expected {@code null} for computed columns
     */
    @Override
    default void addObject(Object newObject)
    {
        if (newObject != null)
        {
            this.throwUnmodifiableColumnException();
        }
    }

    @Override
    default void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregateFunction)
    {
        exceptionByKey("DF_AGG_VAL_TO_COMP_COL").with("columnNane", this.getName()).fire();
    }

    default void throwUnmodifiableColumnException()
    {
        throw exceptionByKey("DF_COMP_COL_MODIFICATION").with("columnNane", this.getName()).getUnsupported();
    }
}
