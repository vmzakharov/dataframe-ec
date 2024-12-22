package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class LongSchemaColumn
extends CsvSchemaColumn
{
    final transient private LongFormatter longFormatter;

    public LongSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.LONG, newPattern);

        this.longFormatter = new LongFormatter(this.getPattern());
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        long longValue = ((DfLongColumn) column).getLong(rowIndex);
        return this.longFormatter.format(longValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfLongColumnStored) dfColumn).addLong(this.longFormatter.parseAsLong(aString), false);
    }
}
