package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.*;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

public class CsvDataSet
extends DataSetAbstract
{
    public static int BUFFER_SIZE = 65_536;

    public static final char SEPARATOR = ',';
    public static final char QUOTE_CHARACTER = '"';

    private final String dataFileName;

    private boolean emptyElementsConvertedToNulls = false;

    private CsvSchema schema;
    private String columnFormatPattern; // todo: refactor so that this is not required

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
        try (BufferedWriter writer = new BufferedWriter(this.createWriter(), BUFFER_SIZE))
        {
            int columnCount = dataFrame.columnCount();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                writer.write(dataFrame.getColumnAt(columnIndex).getName());

                if (columnIndex < columnCount - 1)
                {
                    writer.write(SEPARATOR);
                }
            }
            writer.write('\n');

            int rowCount = dataFrame.rowCount();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    String valueAsLiteral = dataFrame.getColumnAt(columnIndex).getValueAsStringLiteral(rowIndex);
                    writer.write(valueAsLiteral);

                    if (columnIndex < columnCount - 1)
                    {
                        writer.write(SEPARATOR);
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

    private Writer createWriter()
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
        DataFrame df = new DataFrame(this.getName());
        df.enablePooling();

        /*
          expected format:
          header1,header2,header3,...
          "String",123,45.67,...
         */
        try (BufferedReader reader = new BufferedReader(this.createReader(), BUFFER_SIZE))
        {
            String headerLine = reader.readLine();
            String line = reader.readLine();

            if (this.schemaIsNotDefined())
            {
                this.inferSchema(headerLine, line);
            }

            this.getSchema().getColumns().forEach(col -> df.addColumn(col.getName(), col.getType()));

            int columnCount = this.getSchema().columnCount();
            MutableList<String> lineElements = Lists.mutable.withInitialCapacity(columnCount);

            this.parseAndAddLineToDataFrame(df, line, lineElements, columnCount);
            while ((line = reader.readLine()) != null)
            {
                this.parseAndAddLineToDataFrame(df, line, lineElements, columnCount);
            }

            df.seal();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load file as a data frame. File '" + this.dataFileName + "'", e);
        }

        return df;
    }

    private void inferSchema(String headerLine, String firstDataLine)
    {
        MutableList<String> headers = Lists.mutable.of();
        this.splitMindingQsInto(headerLine, headers);

        MutableList<String> elements = Lists.mutable.of();
        this.splitMindingQsInto(firstDataLine, elements);

        int columnCount = elements.size();

        this.schema = new CsvSchema();

        for (int i = 0; i < columnCount; i++)
        {
            String element = elements.get(i);
            ValueType guessedType;
            if (this.surroundedByQuotes(element))
            {
                guessedType = ValueType.STRING;
            }
            else if (this.canParseAsDate(element))
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

            if (this.columnFormatPattern != null)
            {
                this.schema.addColumn(headers.get(i), guessedType, this.columnFormatPattern);
                this.columnFormatPattern = null;
            }
            else
            {
                this.schema.addColumn(headers.get(i), guessedType);
            }
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

    private void parseAndAddLineToDataFrame(DataFrame dataFrame, String line, MutableList<String> elements, int columnCount)
    {
        this.splitMindingQsInto(line, elements);
        for (int i = 0; i < columnCount; i++)
        {
            String element = elements.get(i);
            DfColumn column = dataFrame.getColumnAt(i);

            CsvSchema.Column dataSetColumn = this.getSchema().getColumnAt(i);

            ValueType columnType = dataSetColumn.getType();
            switch (columnType)
            {
                case LONG:
                    ((DfLongColumnStored) column).addLong(Long.parseLong(element));
                    break;
                case DOUBLE:
                    ((DfDoubleColumnStored) column).addDouble(Double.parseDouble(element));
                    break;
                case STRING:
                    ((DfStringColumnStored) column).addString(this.stripQuotesIfAny(element));
                    break;
                case DATE:
                    ((DfDateColumnStored) column).addDate(dataSetColumn.parseAsLocalDate(element));
                    break;
                default:
                    throw new RuntimeException("Don't know what to do with the column type: " + columnType);
            }
        }
    }

    private String stripQuotesIfAny(String aString)
    {
        if (aString == null || !this.surroundedByQuotes(aString))
        {
            return aString;
        }

        return aString.substring(1, aString.length() - 1);
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

    private boolean canParseAsDate(String aString)
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
                this.columnFormatPattern = pattern;
                return true;
            }
            catch (DateTimeParseException e)
            {
                // ignore
            }
        }

        return false;
    }

    private boolean surroundedByQuotes(String aString)
    {
        if (aString.length() < 2)
        {
            return false;
        }

        return (aString.charAt(0) == QUOTE_CHARACTER) && (aString.charAt(aString.length() - 1) == QUOTE_CHARACTER);
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
                        elements.add(substringOrNull(aString, currentTokenStart, index));
                    }
                    // a comma right after a token, so add an empty value
                    elements.add(substringOrNull(aString, index + 1, index + 1));
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
        return aChar == SEPARATOR;
    }

    private boolean isQuote(char aChar)
    {
        return aChar == QUOTE_CHARACTER;
    }

    private String substringOrNull(String aString, int beginIndex, int endIndex)
    {
        if(beginIndex < endIndex)
        {
            return aString.substring(beginIndex, endIndex);
        }

        return this.emptyElementsConvertedToNulls ? null : "";
    }
}
