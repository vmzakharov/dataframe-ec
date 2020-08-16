package org.modelscript.dataset;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.modelscript.dataframe.*;
import org.modelscript.expr.value.ValueType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CsvDataSet
extends DataSetAbstract
{
    public static int BUFFER_SIZE = 65_536;

    public static final char SEPARATOR = ',';
    public static final char QUOTE_CHARACTER = '"';
    private final String dataFileName;

    public CsvDataSet(String dataFileName, String newName)
    {
        super(newName);
        this.dataFileName = dataFileName;
    }

    @Override
    public void openFileForReading()
    {
        File file = new File(this.dataFileName);
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

    }

    public DataFrame loadAsDataFrame()
    {
        DataFrame df = new DataFrame(this.getName());
        df.enablePooling();

        /*
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach(System.out::println);
        }
        */

        /*
          expected format:
          header1,header2,header3,...
          "String",123,45.67,...
         */
        try (BufferedReader reader = new BufferedReader(new FileReader(this.dataFileName), BUFFER_SIZE))
        {
            String line;
            line = reader.readLine();

            MutableList<String> headers = Lists.mutable.of();
            this.splitMindingQsInto(line, headers);

            line = reader.readLine();

            MutableList<String> elements = Lists.mutable.of();
            this.splitMindingQsInto(line, elements);

            MutableList<ValueType> types = Lists.mutable.of();

            int columnCount = elements.size();

            for (int i = 0; i < columnCount; i++)
            {
                String element = elements.get(i);
                ValueType guessedType;
                if (this.surroundedByQuotes(element))
                {
                    guessedType = ValueType.STRING;
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

                types.add(guessedType);
            }

            ListIterate.forEachInBoth(headers, types, df::addColumn);

            MutableList<String> lineElements = Lists.mutable.withInitialCapacity(headers.size());

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

    private void parseAndAddLineToDataFrame(DataFrame dataFrame, String line, MutableList<String> elements, int columnCount)
    {
        this.splitMindingQsInto(line, elements);
        for (int i = 0; i < columnCount; i++)
        {
            DfColumn column = dataFrame.getColumnAt(i);
            ValueType columnType = column.getType();
            String element = elements.get(i);
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
                default:
                    throw new RuntimeException("Don't know what to do with the column type: " + columnType);
            }
        }
    }

    private String stripQuotesIfAny(String aString)
    {
        return this.surroundedByQuotes(aString) ? aString.substring(1, aString.length() - 1) : aString;
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
                    elements.add(substringOrNull(aString, currentTokenStart, index));
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
        return (beginIndex > endIndex) ? null : aString.substring(beginIndex, endIndex);
    }
}
