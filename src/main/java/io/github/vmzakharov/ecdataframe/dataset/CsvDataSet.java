package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateTimeColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class CsvDataSet
extends DataSetAbstract
{
    public static final int BUFFER_SIZE = 65_536;
    public static final int LINE_COUNT_FOR_TYPE_INFERENCE = 100;

    private final Path dataFilePath;

    private boolean emptyElementsConvertedToNulls = false;

    private CsvSchema schema;

    private DateTimeFormatter[] formatters;

    public CsvDataSet(String newDataFileName, String newName)
    {
        this(newDataFileName, newName, null);
    }

    public CsvDataSet(Path newDataFilePath, String newName)
    {
        this(newDataFilePath, newName, null);
    }

    public CsvDataSet(String newDataFileName, String newName, CsvSchema newSchema)
    {
        super(newName);
        this.dataFilePath = Paths.get(newDataFileName);
        this.schema = newSchema;
    }

    public CsvDataSet(Path newDataFilePath, String newName, CsvSchema newSchema)
    {
        super(newName);
        this.dataFilePath = newDataFilePath;
        this.schema = newSchema;
    }

    private String getDataFileName()
    {
        return this.dataFilePath.toString();
    }

    @Override
    public void openFileForReading()
    {
        // Not needed for CSV files
    }

    /**
     * Empty values in the source file (two adjacent separators) will be loaded as null values as this data set is
     * loaded into a data frame. By default, they are treated as empty values of the corresponding column type (e.g.
     * empty strings for string columns, zeroes for numeric columns, etc.)
     * @return this data set
     */
    public CsvDataSet convertEmptyElementsToNulls()
    {
        this.emptyElementsConvertedToNulls = true;
        return this;
    }

    /**
     * Empty values in the source file (two adjacent separators) will be converted to the respective zero or empty
     * values depending on the column type as this data set is loaded into a data frame. This is the default behavior.
     * @return this data set
     */
    public CsvDataSet convertEmptyElementsToValues()
    {
        this.emptyElementsConvertedToNulls = false;
        return this;
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
        // Not needed for CSV files
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

            if (this.schema.hasHeaderLine())
            {
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    writer.write(this.schema.columnAt(columnIndex).getName());

                    if (columnIndex < columnCount - 1)
                    {
                        writer.write(this.getSchema().getSeparator());
                    }
                }
                writer.write('\n');
            }

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
            throw exceptionByKey("CSV_FILE_WRITE_FAIL").get(e);
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
        DfColumn dfColumn = dataFrame.getColumnAt(columnIndex);
        CsvSchemaColumn schemaColumn = this.getSchema().columnAt(columnIndex);

        String valueAsLiteral;

        if (dfColumn.isNull(rowIndex))
        {
            valueAsLiteral = this.schema.hasNullMarker() ? this.schema.getNullMarker() : "";
        }
        else
        {
            switch (dfColumn.getType())
            {
                case LONG:
                    long longValue = ((DfLongColumn) dfColumn).getLong(rowIndex);
                    valueAsLiteral = schemaColumn.getLongFormatter().format(longValue);
                    break;
                case DOUBLE:
                    double doubleValue = ((DfDoubleColumn) dfColumn).getDouble(rowIndex);
                    valueAsLiteral = schemaColumn.getDoubleFormatter().format(doubleValue);
                    break;
                case STRING:
                    String stringValue = dfColumn.getValueAsString(rowIndex);
                    valueAsLiteral = this.schema.getQuoteCharacter() + stringValue + this.schema.getQuoteCharacter();
                    break;
                case DATE:
                    LocalDate dateValue = ((DfDateColumn) dfColumn).getTypedObject(rowIndex);
                    valueAsLiteral = this.formatterForColumn(columnIndex).format(dateValue);
                    break;
                case DATE_TIME:
                    LocalDateTime dateTimeValue = ((DfDateTimeColumn) dfColumn).getTypedObject(rowIndex);
                    valueAsLiteral = this.formatterForColumn(columnIndex).format(dateTimeValue);
                    break;
                default:
                    throw exceptionByKey("CSV_UNSUPPORTED_VAL_TO_STR")
                            .with("valueType", dfColumn.getType()).get();
            }
        }

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
        return new OutputStreamWriter(Files.newOutputStream(this.dataFilePath));
    }

    protected Reader createReader()
    throws IOException
    {
        String fileName = this.dataFilePath.getFileName().toString();

        InputStream fis = Files.newInputStream(this.dataFilePath);

        if (fileName.endsWith(".zip"))
        {
            ZipInputStream zis = new ZipInputStream(fis);
            ZipEntry entry = zis.getNextEntry();
            return new InputStreamReader(zis);
        }

        if (fileName.endsWith(".gz"))
        {
            GZIPInputStream gzis = new GZIPInputStream(fis);
            return new InputStreamReader(gzis);
        }

        return new InputStreamReader(fis);
    }

    /**
     * Load the first lines of the data set as a data frame.
     * If a schema is not specified for this data set, an attempt will be made to infer the schema from the data in
     * the first several lines of the data set.
     * @return a data frame representing the contents of the data set
     */
    public DataFrame loadAsDataFrame()
    {
        return this.loadAsDataFrame(0, true);
    }

    /**
     * load the first lines of the data set as a data frame
     * @param headLineCount the number of lines to load from the beginning of the data set
     * @return a data frame representing the first {@code headLineCount} lines in the data set
     */
    public DataFrame loadAsDataFrame(int headLineCount)
    {
        return this.loadAsDataFrame(headLineCount, false);
    }

    /**
     * Infers schema from the data file defined by this data set. This is done by reading the first several lines of
     * the file and attempting to parse elements using different formats.
     * If the data set already has the schema defined, it gets overridden with the inferred one.
     * By default, the first 100 lines of the file are used to infer column types
     * @return the inferred schema
     */
    public CsvSchema inferSchema()
    {
        return this.inferSchema(LINE_COUNT_FOR_TYPE_INFERENCE);
    }

    /**
     * Infers schema from the data file defined by this data set. This is done by reading the first several lines of
     * the file and attempting to parse elements using different formats. The inferred schema becomes the schema of the
     * data set.
     * If the data set already has the schema defined, it gets overridden with the inferred one.
     * @param numberOfLinesToInferFrom the number of lines from the beginning of the file to be used
     *                                 to infer column types
     * @return the inferred data set schema
     */
    public CsvSchema inferSchema(int numberOfLinesToInferFrom)
    {
        this.schema = new CsvSchema();

        try (BufferedReader reader = new BufferedReader(this.createReader(), BUFFER_SIZE))
        {
            MutableList<String> headers = this.splitMindingQs(reader.readLine()).collect(this::removeSurroundingQuotes);

            MutableList<String> lineBuffer = Lists.mutable.withInitialCapacity(numberOfLinesToInferFrom);

            String dataRow;
            for (int loadedLineCount = 0;
                 loadedLineCount < numberOfLinesToInferFrom && (dataRow = reader.readLine()) != null;
                 loadedLineCount++)
            {
                lineBuffer.add(dataRow);
            }

            this.inferSchema(headers, lineBuffer);
        }
        catch (IOException e)
        {
            exceptionByKey("CSV_INFER_SCHEMA_FAIL").with("fileName", this.getDataFileName()).fire(e);
        }

        return this.schema;
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
            MutableList<String> headers;
            if (this.schema.hasHeaderLine())
            {
                headers = this.splitMindingQs(reader.readLine()).collect(this::removeSurroundingQuotes);

                if (headers.anySatisfy(String::isEmpty))
                {
                    exceptionByKey("CSV_MISSING_COL_HEADER").fire();
                }
            }
            else
            {
                headers = this.schema.getColumns().collect(CsvSchemaColumn::getName);
            }

            String dataRow = reader.readLine();

            if (dataRow == null) // no data, just headers
            {
                if (this.getSchema().columnCount() == 0) // schema does not have columns predefined and there is no data
                {
                    headers.forEach(header -> this.schema.addColumn(header, STRING));
                }

                this.getSchema().getColumns().forEach(col -> df.addColumn(col.getName(), col.getType()));

                return df;
            }

            MutableList<String> lineBuffer = Lists.mutable.withInitialCapacity(LINE_COUNT_FOR_TYPE_INFERENCE);
            lineBuffer.add(dataRow);

            // the schema is empty, need to infer columns properties from the first lineCountForTypeInference columns
            if (this.getSchema().columnCount() == 0)
            {
                int loadedLineCount = 1; // already have one in the buffer
                while (loadedLineCount++ < LINE_COUNT_FOR_TYPE_INFERENCE
                        && (dataRow = reader.readLine()) != null)
                {
                    lineBuffer.add(dataRow);
                }
                this.inferSchema(headers, lineBuffer);
            }
            else if (headers.size() != this.schema.columnCount())
            {
                exceptionByKey("CSV_SCHEMA_HEADER_SIZE_MISMATCH")
                    .with("headerCount", headers.size())
                    .with("schemaColumnCount", this.schema.columnCount())
                    .fire();
            }
            else
            {
                MutableList<String> schemaColumnNames = this.schema.getColumns().collect(CsvSchemaColumn::getName);
                if (!headers.equals(schemaColumnNames))
                {
                    exceptionByKey("CSV_SCHEMA_HEADER_NAME_MISMATCH")
                            .with("headerColumnList", headers.makeString("[", ",", "]"))
                            .with("schemaColumnList", schemaColumnNames.makeString("[", ",", "]"))
                            .fire();
                }
            }

            MutableList<Procedure<String>> columnPopulators = Lists.mutable.of();

            this.getSchema().getColumns().forEach(col -> this.addDataFrameColumn(df, col, columnPopulators));

            int columnCount = this.getSchema().columnCount();
            MutableList<String> lineElements = Lists.mutable.withInitialCapacity(columnCount);

            int lineNumber = 0;

            while (
                    (dataRow = this.getNextLine(lineBuffer, reader, lineNumber)) != null
                    && (loadAllLines || (lineNumber < headLineCount))
            )
            {
                this.parseAndAddLineToDataFrame(dataRow, lineElements, columnCount, columnPopulators);
                lineNumber++;
            }

            df.seal();
        }
        catch (IOException e)
        {
            exceptionByKey("CSV_FILE_LOAD_FAIL").with("fileName", this.getDataFileName()).fire(e);
        }

        return df;
    }

    private String getNextLine(MutableList<String> lineBuffer, BufferedReader reader, int lineNumber)
    throws IOException
    {
        if (lineNumber < lineBuffer.size())
        {
            return lineBuffer.get(lineNumber);
        }

        return reader.readLine();
    }

    private String removeSurroundingQuotes(String aString)
    {
        int size = aString.length();

        if (size > 1)
        {
            if (this.isQuote(aString.charAt(0)) && this.isQuote(aString.charAt(size - 1)))
            {
                return aString.substring(1, size - 1);
            }
        }

        return aString;
    }

    private void addDataFrameColumn(DataFrame df, CsvSchemaColumn schemaCol, MutableList<Procedure<String>> columnPopulators)
    {
        ValueType columnType = schemaCol.getType();

        DfColumn lastColumn = df.newColumn(schemaCol.getName(), columnType);

        switch (columnType)
        {
            case LONG:
                columnPopulators.add(s -> schemaCol.parseAsLongAndAdd(s, lastColumn));
                break;
            case DOUBLE:
                columnPopulators.add(s -> schemaCol.parseAsDoubleAndAdd(s, lastColumn));
                break;
            case STRING:
                columnPopulators.add(s -> lastColumn.addObject(schemaCol.parseAsString(s)));
                break;
            case DATE:
                columnPopulators.add(s -> lastColumn.addObject(schemaCol.parseAsLocalDate(s)));
                break;
            case DATE_TIME:
                columnPopulators.add(s -> lastColumn.addObject(schemaCol.parseAsLocalDateTime(s)));
                break;
            case DECIMAL:
                columnPopulators.add(s -> lastColumn.addObject(schemaCol.parseAsDecimal(s)));
                break;
            default:
                throw exceptionByKey("CSV_POPULATING_BAD_COL_TYPE").with("columnType", columnType).get();
        }
    }

    private void inferSchema(MutableList<String> headers, MutableList<String> topDataLines)
    {
        int columnCount = headers.size();

        ValueType[] types = new ValueType[columnCount];
        String[] formats = new String[columnCount];

        for (int lineIndex = 0; lineIndex < topDataLines.size(); lineIndex++)
        {
            String dataLine = topDataLines.get(lineIndex);

            MutableList<String> elements = this.splitMindingQs(dataLine);

            if (headers.size() != elements.size())
            {
                exceptionByKey("CSV_HEADER_ROW_SIZE_MISMATCH")
                        .with("rowIndex", lineIndex + 1)
                        .with("headerElementCount", headers.size())
                        .with("rowElementCount", elements.size())
                        .fire();
            }

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                String element = elements.get(columnIndex);

                String matchingFormat = null;

                ValueType guessedType;

                // element can be null if it is a null marker or if it is an empty element (0-length)
                if (element == null || element.isEmpty())
                {
                    continue;
                }

                if (this.getSchema().surroundedByQuotes(element))
                {
                    guessedType = STRING;
                }
                else
                {
                    matchingFormat = this.findMatchingDateFormat(element);
                    if (matchingFormat != null)
                    {
                        guessedType = DATE;
                    }
                    else
                    {
                        matchingFormat = this.findMatchingDateTimeFormat(element);
                        if (matchingFormat != null)
                        {
                            guessedType = DATE_TIME;
                        }
                        else if (this.canParseAsLong(element))
                        {
                            guessedType = LONG;
                        }
                        else if (this.canParseAsDouble(element))
                        {
                            guessedType = DOUBLE;
                        }
                        else
                        {
                            guessedType = STRING;
                        }
                    }
                }

                if (types[columnIndex] == null)
                {
                    types[columnIndex] = guessedType;
                    formats[columnIndex] = matchingFormat;
                }
                else if (guessedType == STRING)
                {
                    types[columnIndex] = guessedType;
                }
                else if (guessedType == DOUBLE && types[columnIndex] == LONG)
                {
                    types[columnIndex] = DOUBLE;
                }
                else if (((guessedType == DATE || guessedType == DATE_TIME) && types[columnIndex] == guessedType)
                        && !matchingFormat.equals(formats[columnIndex]))
                {
                    // still a date or dateTime but mismatched formats
                    types[columnIndex] = STRING;
                    formats[columnIndex] = null;
                }
            }
        }

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
        {
            this.schema.addColumn(
                    headers.get(columnIndex),
                    types[columnIndex] == null ? STRING : types[columnIndex],
                    formats[columnIndex]);
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

        if (this.getSchema().columnCount() != elements.size())
        {
            exceptionByKey("CSV_SCHEMA_ROW_SIZE_MISMATCH")
                    .with("schemaColumnCount", this.getSchema().columnCount())
                    .with("rowElementCount", elements.size())
                    .with("dataRow", line)
                    .fire();
        }

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

    private String findMatchingDateTimeFormat(String aString)
    {
        ListIterable<String> dateFormats = Lists.immutable.of("uuuu-M-d'T'H:m:s");

        String trimmed = aString.trim();
        for (int i = 0; i < dateFormats.size(); i++)
        {
            String pattern = dateFormats.get(i);

            try
            {
                DateTimeFormatter candidateFormatter = DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.STRICT);
                LocalDateTime.parse(trimmed, candidateFormatter);
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
                    throw exceptionByKey("CSV_UNBALANCED_QUOTES").with("index", index).with("string", aString).get();
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
