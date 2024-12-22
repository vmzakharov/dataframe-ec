package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateTimeColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

public class DateTimeSchemaColumn
extends CsvSchemaColumn
{
    final transient private DateTimeFormatter dateTimeFormatter;

    public DateTimeSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.DATE_TIME, newPattern);

        if (newPattern == null)
        {
            this.setPattern("uuuu-M-d'T'H:m:s");
        }

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.getPattern()).withResolverStyle(ResolverStyle.STRICT);
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        LocalDateTime dateTimeValue = ((DfDateTimeColumn) column).getTypedObject(rowIndex);
        return this.dateTimeFormatter.format(dateTimeValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        dfColumn.addObject(this.parseAsLocalDateTime(aString));
    }

    private LocalDateTime parseAsLocalDateTime(String aString)
    {
        if (aString == null)
        {
            return null;
        }

        String trimmed = aString.trim();

        return trimmed.isEmpty() ? null : LocalDateTime.parse(trimmed, this.dateTimeFormatter);
    }
}
