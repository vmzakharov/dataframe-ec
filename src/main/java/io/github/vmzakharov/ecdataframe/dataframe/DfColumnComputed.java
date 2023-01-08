package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ErrorReporter;

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
        ErrorReporter.reportAndThrow("Cannot set a value on computed column '" + this.getName() + "'");
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
        ErrorReporter.reportAndThrow("Cannot store aggregated value into a computed column '" + this.getName() + "'");
    }

    default void throwUnmodifiableColumnException()
    {
        throw ErrorReporter.unsupported("Cannot directly modify computed column '" + this.getName() + "'");
    }
}
