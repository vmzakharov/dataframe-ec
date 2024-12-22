package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class StringSchemaColumn
extends CsvSchemaColumn
{
    public StringSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.STRING, newPattern);
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        String stringValue = column.getValueAsString(rowIndex);
        return this.csvSchema().getQuoteCharacter() + stringValue + this.csvSchema().getQuoteCharacter();
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        dfColumn.addObject(this.stripQuotesIfAny(aString));
    }

    private String stripQuotesIfAny(String aString)
    {
        if (aString == null || !this.csvSchema().surroundedByQuotes(aString))
        {
            return aString;
        }

        return aString.substring(1, aString.length() - 1);
    }
}
