package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public abstract class CsvSchemaColumn
{
    private final String name;
    private final ValueType type;
    private String pattern;

    transient private final CsvSchema csvSchema;

    public CsvSchemaColumn(CsvSchema newCsvSchema, String newName, ValueType newType, String newPattern)
    {
        this.csvSchema = newCsvSchema;
        this.name = newName;
        this.type = newType;
        this.pattern = newPattern;
    }

    protected CsvSchema csvSchema()
    {
        return this.csvSchema;
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

    protected void setPattern(String newPattern)
    {
        this.pattern = newPattern;
    }

    abstract public void parseAndAddToColumn(String aString, DfColumn dfColumn);

    abstract public String formatColumnValueAsString(DfColumn column, int rowIndex);
}
