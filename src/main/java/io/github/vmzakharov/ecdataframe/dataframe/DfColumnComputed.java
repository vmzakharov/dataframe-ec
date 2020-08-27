package io.github.vmzakharov.ecdataframe.dataframe;

public interface DfColumnComputed
extends DfColumn
{
    @Override
    default boolean isStored() { return false; }

    String getExpressionAsString();

    @Override
    default void setObject(int rowIndex, Object anObject)
    {
        throw new RuntimeException("Cannot set a value on computed column '" + this.getName() + "'");
    }

    @Override
    default void addEmptyValue()
    {
        // no-op
    }

    @Override
    default void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        throw new RuntimeException("Cannot increment a value on computed column '" + this.getName() + "'");
    }

    default void unmodifiableColumnException()
    {
        throw new UnsupportedOperationException("Cannot modify computed column '" + this.getName() + "'");
    }

    @Override
    default DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        return this.cloneSchemaAndAttachTo(target);
    }
}
