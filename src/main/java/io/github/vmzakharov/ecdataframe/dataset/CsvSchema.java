package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class CsvSchema
{
    private String nullMarker;
    private char separator = ',';
    private char quoteCharacter = '"';
    private boolean hasHeaderLine = true;

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

    public boolean hasHeaderLine()
    {
        return this.hasHeaderLine;
    }

    public CsvSchema hasHeaderLine(boolean newHasHeaderLine)
    {
        this.hasHeaderLine = newHasHeaderLine;
        return this;
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

    public CsvSchema addColumn(String name, ValueType type)
    {
        return this.addColumn(name, type, null);
    }

    public CsvSchema addColumn(String name, ValueType type, String format)
    {
        CsvSchemaColumn newColumn = switch (type)
            {
                case INT: yield new IntSchemaColumn(this, name, format);
                case LONG: yield new LongSchemaColumn(this, name, format);
                case FLOAT: yield new FloatSchemaColumn(this, name, format);
                case DOUBLE: yield new DoubleSchemaColumn(this, name, format);
                case BOOLEAN: yield new BooleanSchemaColumn(this, name, format);
                case STRING: yield new StringSchemaColumn(this, name, format);
                case DATE: yield new DateSchemaColumn(this, name, format);
                case DATE_TIME: yield new DateTimeSchemaColumn(this, name, format);
                case DECIMAL: yield new DecimalSchemaColumn(this, name, format);
                default: throw exceptionByKey("CSV_UNSUPPORTED_COL_TYPE").with("valueType", type).get();
            };

        this.columns.add(newColumn);
        return this;
    }

    public MutableList<CsvSchemaColumn> getColumns()
    {
        return this.columns;
    }

    public int columnCount()
    {
        return this.columns.size();
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
