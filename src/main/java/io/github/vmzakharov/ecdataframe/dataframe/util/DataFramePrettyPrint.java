package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.impl.utility.StringIterate;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;

public class DataFramePrettyPrint
{
    private int maxStringLength = 20;
    private String longFormat = "%d";
    private String doubleFormat = "%.4f";
    private boolean showHorizontalSeparators = false;
    private boolean quoteStrings = true;

    public DataFramePrettyPrint maxStringLength(int newMaxStringLength)
    {
        this.maxStringLength = newMaxStringLength;
        return this;
    }

    public DataFramePrettyPrint longFormat(String newLongFormat)
    {
        this.longFormat = newLongFormat;
        return this;
    }

    public DataFramePrettyPrint doubleFormat(String newDoubleFormat)
    {
        this.doubleFormat = newDoubleFormat;
        return this;
    }

    public DataFramePrettyPrint horizontalSeparators(boolean newShowHorizontalSeparators)
    {
        this.showHorizontalSeparators = newShowHorizontalSeparators;
        return this;
    }

    public DataFramePrettyPrint quoteStrings(boolean newQuoteStrings)
    {
        this.quoteStrings = newQuoteStrings;
        return this;
    }

    public String prettyPrint(DataFrame df)
    {
        int columnCount = df.columnCount();
        int rowCount = df.rowCount();
        String[][] strings = new String[rowCount + 1][columnCount];
        int[] maxValueWidthPerColumn = new int[columnCount];

        df.getColumns().forEachWithIndex((col, i) -> {
            strings[0][i] = this.checkSize(col.getName());
            maxValueWidthPerColumn[i] = Math.max(maxValueWidthPerColumn[i], strings[0][i].length());
        });

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                Object valueAsObject = df.getObject(rowIndex, columnIndex);
                String valueToString;
                if (valueAsObject == null)
                {
                    valueToString = "null";
                }
                else
                {
                    ValueType type = df.getColumnAt(columnIndex).getType();
                    if (type == DOUBLE)
                    {
                        valueToString = String.format(this.doubleFormat, valueAsObject);
                    }
                    else if (type == LONG)
                    {
                        valueToString = String.format(this.longFormat, valueAsObject);
                    }
                    else
                    {
                        valueToString = df.getValueAsString(rowIndex, columnIndex);
                    }

                    valueToString = this.checkSize(valueToString);

                    if (type == STRING && this.quoteStrings)
                    {
                        valueToString = '"' + valueToString + '"';
                    }
                }

                strings[rowIndex + 1][columnIndex] = valueToString;

                maxValueWidthPerColumn[columnIndex] = Math.max(maxValueWidthPerColumn[columnIndex], strings[rowIndex + 1][columnIndex].length());
            }
        }

        String[] formats = new String[columnCount];
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
        {
            formats[columnIndex] =
                    (df.getColumnAt(columnIndex).getType().isNumber() ? "%1$" : "%1$-")
                    + maxValueWidthPerColumn[columnIndex] + "s";
        }

        StringBuilder result = new StringBuilder();

        String separator = null;
        if (this.showHorizontalSeparators)
        {
            StringBuilder sbSeparator = new StringBuilder();
            sbSeparator.append('+');
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                sbSeparator.append(StringIterate.repeat('-', maxValueWidthPerColumn[columnIndex])).append('+');
            }
            sbSeparator.append('\n');

            separator = sbSeparator.toString();

            result.append(separator);
        }

        for (int rowIndex = 0; rowIndex < strings.length; rowIndex++)
        {
            result.append('|');
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                result.append(String.format(formats[columnIndex], strings[rowIndex][columnIndex])).append('|');
            }
            result.append('\n');

            if (this.showHorizontalSeparators)
            {
                result.append(separator);
            }
        }

        return result.toString();
    }

    public String checkSize(String s)
    {
        String result;
        result = s.length() <= this.maxStringLength ? s : s.substring(0, this.maxStringLength - 3) + "...";
        return result;
    }
}
