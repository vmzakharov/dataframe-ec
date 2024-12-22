package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfBooleanColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfBooleanColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class BooleanSchemaColumn
extends CsvSchemaColumn
{
    final transient private BooleanFormatter booleanFormatter;

    public BooleanSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.BOOLEAN, newPattern);

        this.booleanFormatter = new BooleanFormatter(this.getPattern());
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        boolean booleanValue = ((DfBooleanColumn) column).getBoolean(rowIndex);
        return this.booleanFormatter.format(booleanValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfBooleanColumnStored) dfColumn).addBoolean(this.booleanFormatter.parseAsBoolean(aString), false);
    }
}
