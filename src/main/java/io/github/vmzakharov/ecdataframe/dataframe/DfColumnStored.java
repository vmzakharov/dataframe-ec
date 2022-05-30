package io.github.vmzakharov.ecdataframe.dataframe;

public interface DfColumnStored
extends DfColumn
{
    @Override
    default boolean isStored()
    {
        return true;
    }

    void ensureInitialCapacity(int newCapacity);

    @Override
    default void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        if (aggregator.nullsArePoisonous())
        {
            if (this.isNull(targetRowIndex))
            {
                return;
            }

            if (sourceColumn.isNull(sourceRowIndex))
            {
                this.setObject(targetRowIndex, null);
                return;
            }
        }

        this.aggregateValueInto(targetRowIndex, sourceColumn, sourceRowIndex, aggregator);
    }

    /**
     * <b>protected</b> - do not call, to be implemented by the subtypes
     * calls the provided aggregator on a value of <code>sourceColumn</code> at <code>sourceRowIndex</code> with the
     * combined with the current aggregated value at <code>rowIndex</code> of this column
     *
     * @param rowIndex       - the row index in this column at which to store the new aggregated value
     * @param sourceColumn   - the column from which to extract the new value to be aggregated
     * @param sourceRowIndex - the row index of the new value to be aggregated
     * @param aggregator     - the aggregate function to be applied to the existing aggregate value in this column and
     *                       the new value extracted from the <code>sourceColumn</code>
     */
    void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator);
}
