package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

public class CsvSchema
{
    private String nullMarker;
    private char separator = ',';
    private char quoteCharacter = '"';

    private final MutableList<Column> columns = Lists.mutable.of();

    public String getNullMarker()
    {
        return this.nullMarker;
    }

    public CsvSchema nullMarker(String nullMarker)
    {
        this.nullMarker = nullMarker;
        return this;
    }

    public boolean hasNullMarker()
    {
        return this.nullMarker != null;
    }

    public char getSeparator()
    {
        return this.separator;
    }

    public CsvSchema separator(char separator)
    {
        this.separator = separator;
        return this;
    }

    public char getQuoteCharacter()
    {
        return this.quoteCharacter;
    }

    public CsvSchema quoteCharacter(char quoteCharacter)
    {
        this.quoteCharacter = quoteCharacter;
        return this;
    }

    public void addColumn(String name, ValueType type)
    {
        this.columns.add(new Column(name, type));
    }

    public void addColumn(String name, ValueType type, String format)
    {
        this.columns.add(new Column(name, type, format));
    }

    public MutableList<Column> getColumns()
    {
        return this.columns;
    }

    public Column getColumnAt(int i)
    {
        return this.columns.get(i);
    }

    public class Column
    {
        private final String name;
        private final ValueType type;
        private final String pattern;
        transient private DateTimeFormatter dateTimeFormatter;

        public Column(String newName, ValueType newType)
        {
            this(newName, newType, null);
        }

        public Column(String newName, ValueType newType, String newPattern)
        {
            this.name = newName;
            this.type = newType;
            this.pattern = newPattern == null ? "uuuu-MM-dd" : newPattern;

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
            return stripQuotesIfAny(aString);
        }
    }

    public int columnCount()
    {
        return this.columns.size();
    }

    public String stripQuotesIfAny(String aString)
    {
        if (aString == null || !this.surroundedByQuotes(aString))
        {
            return aString;
        }

        return aString.substring(1, aString.length() - 1);
    }

    public boolean surroundedByQuotes(String aString)
    {
        if (aString.length() < 2)
        {
            return false;
        }

        return (aString.charAt(0) == this.getQuoteCharacter()) && (aString.charAt(aString.length() - 1) == this.getQuoteCharacter());
    }
}
