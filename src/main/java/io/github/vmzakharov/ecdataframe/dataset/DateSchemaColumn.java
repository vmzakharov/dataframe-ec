package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Locale;

public class DateSchemaColumn
extends CsvSchemaColumn
{
    final transient private DateTimeFormatter dateTimeFormatter;

    public DateSchemaColumn(CsvSchema newCsvSchema, String newName, String newPattern)
    {
        super(newCsvSchema, newName, ValueType.DATE, newPattern);

        if (newPattern == null)
        {
            this.setPattern("uuuu-M-d");
        }

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.getPattern(), Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    }

    @Override
    public String formatColumnValueAsString(DfColumn column, int rowIndex)
    {
        LocalDate dateValue = ((DfDateColumn) column).getTypedObject(rowIndex);
        return this.dateTimeFormatter.format(dateValue);
    }

    @Override
    public void parseAndAddToColumn(String aString, DfColumn dfColumn)
    {
        dfColumn.addObject(this.parseAsLocalDate(aString));
    }

    private LocalDate parseAsLocalDate(String aString)
    {
        if (aString == null)
        {
            return null;
        }

        String trimmed = aString.trim();

        return trimmed.isEmpty() ? null : LocalDate.parse(trimmed, this.dateTimeFormatter);
    }
}
