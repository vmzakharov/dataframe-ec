package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class DoubleSchemaColumn
extends CsvSchemaColumn
{
    final transient private DoubleFormatter doubleFormatter;

    public DoubleSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.DOUBLE, newPattern);

        this.doubleFormatter = new DoubleFormatter(this.getPattern());
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        double doubleValue = ((DfDoubleColumn) column).getDouble(rowIndex);
        return this.doubleFormatter.format(doubleValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfDoubleColumnStored) dfColumn).addDouble(this.doubleFormatter.parseAsDouble(aString));
    }
}
