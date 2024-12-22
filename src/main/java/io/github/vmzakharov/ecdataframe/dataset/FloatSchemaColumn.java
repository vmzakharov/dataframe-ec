package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class FloatSchemaColumn
extends CsvSchemaColumn
{
    final transient private FloatFormatter floatFormatter;

    public FloatSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.FLOAT, newPattern);

        this.floatFormatter = new FloatFormatter(this.getPattern());
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        float floatValue = ((DfFloatColumn) column).getFloat(rowIndex);
        return this.floatFormatter.format(floatValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfFloatColumnStored) dfColumn).addFloat(this.floatFormatter.parseAsFloat(aString));
    }
}
