package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

public class CsvSchemaColumn
{
    private final String name;
    private final ValueType type;
    private final String pattern;

    transient private final CsvSchema csvSchema;
    transient private DateTimeFormatter dateTimeFormatter;

    public CsvSchemaColumn(CsvSchema newCsvSchema, String newName, ValueType newType, String newPattern)
    {
        this.csvSchema = newCsvSchema;
        this.name = newName;
        this.type = newType;
        this.pattern = (this.type.isDate() && newPattern == null) ? "uuuu-MM-dd" : newPattern;

        if (this.type.isDate() || this.type.isDateTime())
        {
            this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.pattern).withResolverStyle(ResolverStyle.STRICT);
        }
    }

    public String getName()
    {
        return this.name;
    }

    public ValueType getType()
    {
        return this.type;
    }

    public String getPattern()
    {
        return this.pattern;
    }

    public LocalDate parseAsLocalDate(String aString)
    {
        if (aString == null)
        {
            return null;
        }

        String trimmed = aString.trim();

        return trimmed.length() == 0 ? null : LocalDate.parse(trimmed, this.dateTimeFormatter);
    }

    public double parseAsDouble(String aString)
    {
        if (aString == null || aString.length() == 0)
        {
            return 0.0;
        }

        return Double.parseDouble(aString);
    }

    public long parseAsLong(String aString)
    {
        if (aString == null || aString.length() == 0)
        {
            return 0L;
        }

        return Long.parseLong(aString);
    }

    public String parseAsString(String aString)
    {
        return csvSchema.stripQuotesIfAny(aString);
    }
}
