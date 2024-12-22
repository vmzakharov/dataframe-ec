package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.math.BigDecimal;

public class DecimalSchemaColumn
extends CsvSchemaColumn
{
    final transient private DecimalFormatter decimalFormatter;

    public DecimalSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.DECIMAL, newPattern);
        this.decimalFormatter = new DecimalFormatter(newPattern);
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        BigDecimal decimalValue = ((DfDecimalColumn) column).getTypedObject(rowIndex);
        return this.decimalFormatter.format(decimalValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        dfColumn.addObject(this.decimalFormatter.parseAsDecimal(aString));
    }
}
