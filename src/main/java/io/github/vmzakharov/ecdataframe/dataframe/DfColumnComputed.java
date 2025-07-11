package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

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
        this.getDataFrame().setEvalContextRowIndex(rowIndex);

        return this.getExpression().evaluate(this.getDataFrame().getEvalVisitor());
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
        throw exceptionByKey("DF_CALC_COL_MODIFICATION").with("columnName", this.getName()).getUnsupported();
    }

    @Override
    default int getSize()
    {
        return this.getDataFrame().rowCount();
    }
}
