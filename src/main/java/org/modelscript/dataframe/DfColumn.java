package org.modelscript.dataframe;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

public interface DfColumn
{
    String getName();

    String getValueAsString(int rowIndex);

    void addObject(Object newObject);

    void addValue(Value value);

    String getValueAsStringLiteral(int rowIndex);

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

    void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex);

    void cloneSchemaAndAttachTo(DataFrame attachTo);

    void addRowToColumn(int rowIndex, DfColumn target);

    void seal();
}
