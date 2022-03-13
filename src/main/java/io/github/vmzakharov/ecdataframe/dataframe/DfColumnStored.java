package io.github.vmzakharov.ecdataframe.dataframe;

public interface DfColumnStored
extends DfColumn
{
    @Override
    default boolean isStored() { return true; }

    void ensureInitialCapacity(int newCapacity);

    @Override
    default void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        // nulls are "poisonous"
        if (this.isNull(targetRowIndex))
        {
            return;
        }

        if (sourceColumn.isNull(sourceRowIndex))
        {
            this.setObject(targetRowIndex, null);
        }
        else
        {
            this.aggregateValueInto(targetRowIndex, sourceColumn, sourceRowIndex, aggregator);
        }
    }

    /**
     * <b>protected</b> - do not call, to be implemented by the subtypes
     * calls the provided aggregator on a value of <code>sourceColumn</code> at <code>sourceRowIndex</code>
     * with the combined with the current aggregated value at <code>rowIndex</code> of this column
     * @param rowIndex
     * @param sourceColumn
     * @param sourceRowIndex
     * @param aggregator
     */
    void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator);
}
