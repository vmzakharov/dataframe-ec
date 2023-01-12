package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exception;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public interface DfColumn
{
    String getName();

    String getValueAsString(int rowIndex);

    default String getValueAsStringLiteral(int rowIndex)
    {
        return this.isNull(rowIndex) ? "" : this.getValueAsString(rowIndex);
    }

    void addObject(Object newObject);

    void addValue(Value value);

    boolean isNull(int rowIndex);

    Object getObject(int rowIndex);

    DataFrame getDataFrame();

    Value getValue(int rowIndex);

    ValueType getType();

    boolean isStored();

    default boolean isComputed()
    {
        return !this.isStored();
    }

    int getSize();

    void setObject(int rowIndex, Object anObject);

    void addEmptyValue();

    default Object aggregate(AggregateFunction aggregator)
    {
        throw exception("Aggregation ${aggregatorName} (${aggregationDescription}) cannot be performed "
                                   + "on column ${columnName} of type ${columnType}")
                .with("aggregationName", aggregator.getName())
                .with("aggregationDescription", aggregator.getDescription())
                .with("columnName", this.getName())
                .with("columnType", this.getType())
                .get();
    }

    void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregateFunction);

    DfColumn cloneSchemaAndAttachTo(DataFrame attachTo);

    DfColumn cloneSchemaAndAttachTo(DataFrame attachTo, String newName);

    void addRowToColumn(int rowIndex, DfColumn target);

    default void seal()
    {
        // nothing
    }

    default void enablePooling()
    {
        // nothing
    }

    DfColumn mergeWithInto(DfColumn other, DataFrame target);

    DfColumn copyTo(DataFrame target);

    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        throw exceptionByKey("DF_NO_COL_COMPARATOR")
                .with("columnName", this.getName())
                .with("columnType", this.getType())
                .getUnsupported();
    }

    default int dataFrameRowIndex(int virtualRowIndex)
    {
        return this.getDataFrame().rowIndexMap(virtualRowIndex);
    }
}
