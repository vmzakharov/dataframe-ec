package org.modelscript.dataset;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.modelscript.dataframe.*;
import org.modelscript.expr.value.ValueType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CsvDataSet
        extends DataSetAbstract
{
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

    public DataFrame loadAsDataFrame()
    {
        DataFrame df = new DataFrame(this.getName());

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
        try (BufferedReader reader = new BufferedReader(new FileReader(this.dataFileName)))
        {
            String line;
            line = reader.readLine();
            String[] headers = this.splitMindingQs(line);

            line = reader.readLine();

            String[] elements = this.splitMindingQs(line);
            ValueType[] types = new ValueType[headers.length];

            for (int i = 0; i < elements.length; i++)
            {
                String element = elements[i];
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

                types[i] = guessedType;
            }

            ArrayIterate.forEachInBoth(headers, types, df::addColumn);

            this.parseAndAddLineToDataFrame(df, line);
            while ((line = reader.readLine()) != null)
            {
                this.parseAndAddLineToDataFrame(df, line);
            }

            df.seal();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load file as a data frame. File '" + this.dataFileName + "'", e);
        }

        return df;
    }

    private void parseAndAddLineToDataFrame(DataFrame dataFrame, String line)
    {
        String[] elements = this.splitMindingQs(line);
        for (int i = 0; i < elements.length; i++)
        {
            DfColumn column = dataFrame.getColumnAt(i);
            ValueType columnType = column.getType();
            String element = elements[i];
            switch (columnType)
            {
                case LONG:
                    ((DfLongColumnStored) column).addLong(Long.parseLong(element));
                    break;
                case DOUBLE:
                    ((DfDoubleColumnStored) column).addDouble(Double.parseDouble(element));
                    break;
                case STRING:
                    ((DfStringColumnStored) column).addString(this.stripQuotesIfAny(element)) ;
                    break;
                default:
                    throw new RuntimeException("Ay, Carrumba!");
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

    public String[] splitMindingQs(String aString)
    {
        MutableList<String> elements = Lists.mutable.of();

        int currentTokenStart = 0;
        boolean insideQuotes = false;
        boolean initialBlanks = true;
        boolean closedQuote = false;

        for (int index = 0; index < aString.length(); index++)
        {
            char curChar = aString.charAt(index);

            boolean endOfLine = index == aString.length() - 1;
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
                    elements.add(substringOrNull(aString, currentTokenStart, index + 1));
                }
            }
            else if (insideQuotes)
            {
                if (this.isQuote(curChar))
                {
                    insideQuotes = false;
                    closedQuote = true;
                    elements.add(substringOrNull(aString, currentTokenStart, index + 1));
                    currentTokenStart = index + 1;
                }
            }
//            else if (closedQuote)
//            {
//                closedQuote = false;
//                if (!this.isTokenSeparator(aString.charAt(index)))
//                {
//                    this.throwBadFormat("Unexpected character '" + aString.charAt(index + 1) + "' at position " + (index + 1) + ", was hoping for a separator or end of line.");
//                }
//                currentTokenStart = index + 1;
//            }
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

        return elements.toArray(new String[elements.size()]);
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
