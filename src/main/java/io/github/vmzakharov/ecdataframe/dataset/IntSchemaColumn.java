package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class IntSchemaColumn
extends CsvSchemaColumn
{
    final transient private IntFormatter intFormatter;

    public IntSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.INT, newPattern);

        this.intFormatter = new IntFormatter(this.getPattern());
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        int intValue = ((DfIntColumn) column).getInt(rowIndex);
        return this.intFormatter.format(intValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfIntColumnStored) dfColumn).addInt(this.intFormatter.parseAsInt(aString), false);
    }
}
