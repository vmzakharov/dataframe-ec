package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

public class CsvSchema
{
    private String nullMarker;
    private char separator = ',';
    private char quoteCharacter = '"';

    private final MutableList<CsvSchemaColumn> columns = Lists.mutable.of();

    public String getNullMarker()
    {
        return this.nullMarker;
    }

    public CsvSchema nullMarker(String newNullMarker)
    {
        this.nullMarker = newNullMarker;
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

    public CsvSchema separator(char newSeparator)
    {
        this.separator = newSeparator;
        return this;
    }

    public char getQuoteCharacter()
    {
        return this.quoteCharacter;
    }

    public CsvSchema quoteCharacter(char newQuoteCharacter)
    {
        this.quoteCharacter = newQuoteCharacter;
        return this;
    }

    public void addColumn(String name, ValueType type)
    {
        this.addColumn(name, type, null);
    }

    public void addColumn(String name, ValueType type, String format)
    {
        CsvSchemaColumn newColumn = new CsvSchemaColumn(this, name, type, format);
        this.columns.add(newColumn);
    }

    public MutableList<CsvSchemaColumn> getColumns()
    {
        return this.columns;
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

    public CsvSchemaColumn columnAt(int columnIndex)
    {
        return this.columns.get(columnIndex);
    }
}
