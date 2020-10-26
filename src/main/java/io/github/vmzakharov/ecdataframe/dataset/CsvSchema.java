package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

public class CsvSchema
{
    private MutableList<Column> columns = Lists.mutable.of();

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
            this.pattern = newPattern;

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
    }

    public int columnCount()
    {
        return this.columns.size();
    }
}
