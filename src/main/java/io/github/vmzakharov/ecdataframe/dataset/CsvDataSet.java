package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfStringColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.ErrorReporter;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

public class CsvDataSet
extends DataSetAbstract
{
    public static final int BUFFER_SIZE = 65_536;

    private final String dataFileName;

    private boolean emptyElementsConvertedToNulls = false;

    private CsvSchema schema;

    private DateTimeFormatter[] formatters;

    public CsvDataSet(String dataFileName, String newName)
    {
        this(dataFileName, newName, null);
    }

    public CsvDataSet(String dataFileName, String newName, CsvSchema newSchema)
    {
        super(newName);
        this.dataFileName = dataFileName;
        this.schema = newSchema;
    }

    @Override
    public void openFileForReading()
    {
        File file = new File(this.dataFileName);
    }

    public void convertEmptyElementsToNulls()
    {
        this.emptyElementsConvertedToNulls = true;
    }

    @Override
    public Object next()
    {
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public void close()
    {
//
    }

    public void write(DataFrame dataFrame)
    {
        if (this.schemaIsNotDefined())
        {
            this.schema = this.schemaFromDataFrame(dataFrame);
        }

        try (BufferedWriter writer = new BufferedWriter(this.createWriter(), BUFFER_SIZE))
        {
            int columnCount = dataFrame.columnCount();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                writer.write(this.schema.columnAt(columnIndex).getName());

                if (columnIndex < columnCount - 1)
                {
                    writer.write(this.getSchema().getSeparator());
                }
            }
            writer.write('\n');

            int rowCount = dataFrame.rowCount();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    this.writeValue(writer, dataFrame, rowIndex, columnIndex);

                    if (columnIndex < columnCount - 1)
                    {
                        writer.write(this.getSchema().getSeparator());
                    }
                }
                writer.write('\n');
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed write data frame to '" + this.dataFileName + "'", e);
        }
    }

    private CsvSchema schemaFromDataFrame(DataFrame dataFrame)
    {
        CsvSchema dfSchema = new CsvSchema();
        dataFrame.getColumns().forEach(e -> dfSchema.addColumn(e.getName(), e.getType()));
        return dfSchema;
    }

    private void writeValue(Writer writer, DataFrame dataFrame, int rowIndex, int columnIndex)
    throws IOException
    {
        DfColumn column = dataFrame.getColumnAt(columnIndex);

        String valueAsLiteral;
        switch (column.getType())
        {
            case LONG:
            case DOUBLE:
                valueAsLiteral = column.getValueAsStringLiteral(rowIndex);
                break;
            case STRING:
                String stringValue = column.getValueAsString(rowIndex);
                valueAsLiteral = stringValue == null ? "" : this.schema.getQuoteCharacter() + stringValue + this.schema.getQuoteCharacter();
                break;
            case DATE:
                LocalDate dateValue = ((DfDateColumn) column).getDate(rowIndex);
                valueAsLiteral = dateValue == null ? "" : this.formatterForColumn(columnIndex).format(dateValue);
                break;
            default:
                ErrorReporter.reportAndThrow("Do not know how to convert value of type " + column.getType() + " to a string");
                valueAsLiteral = null;
        }

        assert valueAsLiteral != null;
        writer.write(valueAsLiteral);
    }

    private DateTimeFormatter formatterForColumn(int columnIndex)
    {
        if (this.formatters == null)
        {
            this.formatters = new DateTimeFormatter[this.schema.columnCount()];
        }

        if (this.formatters[columnIndex] == null)
        {
            String pattern = this.schema.columnAt(columnIndex).getPattern();
            this.formatters[columnIndex] = DateTimeFormatter.ofPattern(pattern);
        }

        return this.formatters[columnIndex];
    }

    protected Writer createWriter()
    throws IOException
    {
        return new FileWriter(this.dataFileName);
    }

    protected Reader createReader()
    throws IOException
    {
        return new FileReader(this.dataFileName);
    }

    public DataFrame loadAsDataFrame()
    {
        return this.loadAsDataFrame(0,true);
    }

    public DataFrame loadAsDataFrame(int headLineCount)
    {
        return this.loadAsDataFrame(headLineCount, false);
    }

    private DataFrame loadAsDataFrame(int headLineCount, boolean loadAllLines)
    {
        DataFrame df = new DataFrame(this.getName());
        df.enablePooling();

        /*
          expected format:
          header1,header2,header3,...
          "String",123,45.67,...
         */
        if (this.schemaIsNotDefined())
        {
            this.schema = new CsvSchema(); // provides default separators, quote characters, etc.
        }

        try (BufferedReader reader = new BufferedReader(this.createReader(), BUFFER_SIZE))
        {
            MutableList<String> headers = this.splitMindingQs(reader.readLine());

            String dataRow = reader.readLine();

            if (dataRow == null) // no data, just headers
            {
                if (this.getSchema().columnCount() == 0)
                {
                    headers.forEach(header -> this.schema.addColumn(header, ValueType.STRING));
                }

                this.getSchema().getColumns().forEach(col -> df.addColumn(col.getName(), col.getType()));

                return df;
            }

            MutableList<String> firstRowElements = this.splitMindingQs(dataRow);

            ErrorReporter.reportAndThrow(headers.size() != firstRowElements.size(),
                    "The number of elements in the header does not match the number of elements in the first data row ("
                    + headers.size() + ", " + firstRowElements.size() + ")");

            if (this.getSchema().columnCount() == 0)
            {
                this.inferSchema(headers, firstRowElements);
            }

            ErrorReporter.reportAndThrow(this.getSchema().columnCount() != firstRowElements.size(),
                    "The number of columns in the schema does not match the number of elements in the first data row ("
                    + this.getSchema().columnCount() + ", " + firstRowElements.size() + ")");

            MutableList<Procedure<String>> columnPopulators = Lists.mutable.of();

            this.getSchema().getColumns().forEach(col -> this.addDataFrameColumn(df, col, columnPopulators));

            int columnCount = this.getSchema().columnCount();
            MutableList<String> lineElements = Lists.mutable.withInitialCapacity(columnCount);

            if (loadAllLines)
            {
                do
                {
                    this.parseAndAddLineToDataFrame(dataRow, lineElements, columnCount, columnPopulators);
                }
                while ((dataRow = reader.readLine()) != null);
            }
            else if (headLineCount > 0)
            {
                int lineCount = 0;

                do
                {
                    lineCount++;
                    this.parseAndAddLineToDataFrame(dataRow, lineElements, columnCount, columnPopulators);
                }
                while ((dataRow = reader.readLine()) != null && lineCount != headLineCount);
            }

            df.seal();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load file as a data frame. File '" + this.dataFileName + "'", e);
        }

        return df;
    }

    private void addDataFrameColumn(DataFrame df, CsvSchemaColumn col, MutableList<Procedure<String>> columnPopulators)
    {
        ValueType columnType = col.getType();

        df.addColumn(col.getName(), col.getType());

        DfColumn lastColumn = df.getColumnAt(df.columnCount() - 1);

        switch (columnType)
        {
            case LONG:
                columnPopulators.add(s -> ((DfLongColumnStored) lastColumn).addLong(col.parseAsLong(s)));
                break;
            case DOUBLE:
                columnPopulators.add(s -> ((DfDoubleColumnStored) lastColumn).addDouble(col.parseAsDouble(s)));
                break;
            case STRING:
                columnPopulators.add(s -> ((DfStringColumnStored) lastColumn).addString(col.parseAsString(s)));
                break;
            case DATE:
                columnPopulators.add(s -> ((DfDateColumnStored) lastColumn).addDate(col.parseAsLocalDate(s)));
                break;
            default:
                throw new RuntimeException("Don't know what to do with the column type: " + columnType);
        }
    }

    private void inferSchema(MutableList<String> headers, MutableList<String> elements)
    {
        int columnCount = elements.size();

        for (int i = 0; i < columnCount; i++)
        {
            String element = elements.get(i);
            ValueType guessedType;
            String matchingFormat = null;

            if (this.getSchema().surroundedByQuotes(element))
            {
                guessedType = ValueType.STRING;
            }
            else
            {
                matchingFormat = this.findMatchingDateFormat(element);

                if (matchingFormat != null)
                {
                    guessedType = ValueType.DATE;
                }
                else if (this.canParseAsLong(element))
                {
                    guessedType = ValueType.LONG;
                }
                else if (this.canParseAsDouble(element))
                {
                    guessedType = ValueType.DOUBLE;
                }
                else
                {
                    guessedType = ValueType.STRING;
                }
            }
            this.schema.addColumn(headers.get(i), guessedType, matchingFormat);
        }
    }

    private boolean schemaIsNotDefined()
    {
        return this.schema == null;
    }

    public CsvSchema getSchema()
    {
        return this.schema;
    }

    private void parseAndAddLineToDataFrame(String line, MutableList<String> elements, int columnCount, MutableList<Procedure<String>> columnPopulators)
    {
        this.splitMindingQsInto(line, elements);
        for (int i = 0; i < columnCount; i++)
        {
            String element = elements.get(i);

            if (this.getSchema().hasNullMarker())
            {
                if (this.getSchema().getNullMarker().equals(element))
                element = null;
            }

            columnPopulators.get(i).accept(element);
        }
    }

    private boolean canParseAsLong(String aString)
    {
        try
        {
            Long.parseLong(aString);
            return true;
        }
        catch (NumberFormatException e)
        {
            return false;
        }
    }

    private boolean canParseAsDouble(String aString)
    {
        try
        {
            Double.parseDouble(aString);
            return true;
        }
        catch (NumberFormatException e)
        {
            return false;
        }
    }

    private String findMatchingDateFormat(String aString)
    {
        ListIterable<String> dateFormats = Lists.immutable.of("uuuu/M/d", "uuuu-M-d", "M/d/uuuu");

        String trimmed = aString.trim();
        for (int i = 0; i < dateFormats.size(); i++)
        {
            String pattern = dateFormats.get(i);

            try
            {
                DateTimeFormatter candidateFormatter = DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT);
                LocalDate.parse(trimmed, candidateFormatter);
                return pattern;
            }
            catch (DateTimeParseException e)
            {
                // ignore
            }
        }

        return null;
    }

    private MutableList<String> splitMindingQs(String aString)
    {
        MutableList<String> elements = Lists.mutable.of();
        this.splitMindingQsInto(aString, elements);
        return elements;
    }

    public void splitMindingQsInto(String aString, MutableList<String> elements)
    {
        elements.clear();

        int currentTokenStart = 0;
        boolean insideQuotes = false;
        boolean initialBlanks = true;
        boolean closedQuote = false;
        int charCount = aString.length();

        for (int index = 0; index < charCount; index++)
        {
            char curChar = aString.charAt(index);

            boolean endOfLine = index == charCount - 1;
            if (endOfLine)
            {
                if (insideQuotes && !this.isQuote(curChar))
                {
                    this.throwBadFormat("Unbalanced quotes at index " + index + " in " + aString);
                }
                if (this.isTokenSeparator(curChar))
                {
                    if (!closedQuote) // unquoted token followed by an empty token, add the current token first
                    {
                        elements.add(this.substringOrNull(aString, currentTokenStart, index));
                    }
                    // a comma right after a token, so add an empty value
                    elements.add(this.substringOrNull(aString, index + 1, index + 1));
                }
                else
                {
                    elements.add(this.substringOrNull(aString, currentTokenStart, index + 1));
                }
            }
            else if (insideQuotes)
            {
                if (this.isQuote(curChar))
                {
                    insideQuotes = false;
                    closedQuote = true;
                    elements.add(this.substringOrNull(aString, currentTokenStart, index + 1));
                    currentTokenStart = index + 1;
                }
            }
            else if (this.isTokenSeparator(curChar))
            {
                if (!closedQuote)
                {
                    elements.add(this.substringOrNull(aString, currentTokenStart, index));
                }
                closedQuote = false;
                initialBlanks = true;
                currentTokenStart = index + 1;
            }
            else if (initialBlanks)
            {
                if (!Character.isSpaceChar(curChar))
                {
                    initialBlanks = false;
                    if (this.isQuote(curChar))
                    {
                        insideQuotes = true;
                        currentTokenStart = index;
                    }
                }
            }
        }
    }

    private void throwBadFormat(String message)
    {
        throw new RuntimeException(message);
    }

    private boolean isTokenSeparator(char aChar)
    {
        return aChar == this.getSchema().getSeparator();
    }

    private boolean isQuote(char aChar)
    {
        return aChar == this.getSchema().getQuoteCharacter();
    }

    private String substringOrNull(String aString, int beginIndex, int endIndex)
    {
        if (beginIndex < endIndex)
        {
            return aString.substring(beginIndex, endIndex);
        }

        return this.emptyElementsConvertedToNulls ? null : "";
    }
}
