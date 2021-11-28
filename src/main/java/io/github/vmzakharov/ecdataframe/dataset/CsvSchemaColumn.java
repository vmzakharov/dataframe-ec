package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.primitive.DoubleFunction;
import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.regex.Pattern;

public class CsvSchemaColumn
{
    private final String name;
    private final ValueType type;
    private final String pattern;

    transient private final CsvSchema csvSchema;
    transient private DateTimeFormatter dateTimeFormatter;
    transient private DoubleFormatter doubleFormatter;
    transient private LongFormatter longFormatter;

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
        else if (this.type.isDouble())
        {
            this.doubleFormatter = new DoubleFormatter(this.pattern);
        }
        else if (this.type.isLong())
        {
            this.longFormatter = new LongFormatter(this.pattern);
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

        return trimmed.isEmpty() ? null : LocalDate.parse(trimmed, this.dateTimeFormatter);
    }

    public void parseAsDoubleAndAdd(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfDoubleColumnStored) dfColumn).addDouble(this.doubleFormatter.parseAsDouble(aString));
    }

    public void parseAsLongAndAdd(String aString, DfColumn dfColumn)
    {
        if (aString == null)
        {
            dfColumn.addEmptyValue();
            return;
        }

        ((DfLongColumnStored) dfColumn).addLong(this.longFormatter.parseAsLong(aString), false);
    }

    public String parseAsString(String aString)
    {
        return this.csvSchema.stripQuotesIfAny(aString);
    }

    public DoubleFormatter getDoubleFormatter()
    {
        return this.doubleFormatter;
    }

    public LongFormatter getLongFormatter()
    {
        return this.longFormatter;
    }
}
