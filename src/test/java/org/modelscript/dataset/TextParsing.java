package org.modelscript.dataset;

import org.eclipse.collections.impl.utility.ArrayIterate;
import org.junit.Assert;
import org.junit.Test;

public class TextParsing
{
    private final CsvDataSet dataSet = new CsvDataSet("Foo", "Bar");

    @Test
    public void simpleString()
    {
        this.validateSplitting("a,b,c", new String[] {"a", "b", "c"});
    }

    @Test
    public void splitStringWithSpacesAndEmptyValues()
    {
        this.validateSplitting(
                "eenie, meenie  ,  , miny,, moe",
                new String[] {"eenie"," meenie  ","  "," miny", ""," moe"});
    }

    @Test
    public void splitStringWithFirstAndLastEmptyValues()
    {
        this.validateSplitting(
                ",eenie, meenie  ,  , miny,, moe,",
                new String[] {"", "eenie"," meenie  ","  "," miny", ""," moe",""});
    }

    @Test
    public void stringWithFirstAndLastBlankValues()
    {
        this.validateSplitting(" ,eenie, meenie,  ", new String[] {" ", "eenie"," meenie", "  "});
    }

    @Test
    public void stringWithQuotesAndFirstAndLastBlankValues()
    {
        this.validateSplitting(" ,\"eenie\",\" meenie\",  ", new String[] {" ", "\"eenie\"","\" meenie\"", "  "});
    }

    @Test
    public void simpleStringWithQuotes()
    {
        this.validateSplitting("\"aa\",\"bb\",\"cc\"", new String[] {"\"aa\"", "\"bb\"","\"cc\""});
    }

    @Test
    public void simpleStringWithQuotesAndSpacesInVariousPlaces()
    {
        this.validateSplitting(
                "  \"aa\", \"bb\", \"\",\" c c \",",
                new String[] {"\"aa\"", "\"bb\"", "\"\"", "\" c c \"", ""});
    }

    @Test
    public void mixedQuotedTokensAndNot()
    {
        this.validateSplitting(
                "  \"aa\", bb, \"\",\" c c \",  dd ", 
                new String[] {"\"aa\"", " bb", "\"\"", "\" c c \"", "  dd "});
    }

    private void validateSplitting(String input, String[] expected)
    {
        String[] result = this.dataSet.splitMindingQs(input);

//        System.out.println("----------");
//        System.out.println("i> [" + input + "]");
//        System.out.print("e> ");
//        printArray(expected);
//        System.out.print("a> ");
//        printArray(result);

        Assert.assertArrayEquals(expected, result);
    }
    
    public void printArray(String[] a)
    {
        System.out.println(ArrayIterate.collect(a, e -> (e == null) ? "null" : ('[' + e + ']')).makeString());
    }
}
