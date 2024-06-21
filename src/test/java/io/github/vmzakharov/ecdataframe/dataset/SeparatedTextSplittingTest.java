package io.github.vmzakharov.ecdataframe.dataset;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SeparatedTextSplittingTest
{
    private CsvDataSet dataSet;

    @BeforeEach
    public void configureDataSet()
    {
        this.dataSet = new CsvDataSet("Foo", "Bar", new CsvSchema());
    }

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
                new String[] {"eenie", " meenie  ", "  ", " miny", "", " moe"});
    }

    @Test
    public void splitStringWithNulls()
    {
        this.dataSet.convertEmptyElementsToNulls();
        this.validateSplitting(
                "eenie,, miny, , moe,",
                new String[] {"eenie", null, " miny", " ", " moe", null});
    }

    @Test
    public void splitStringWithFirstAndLastEmptyValues()
    {
        this.validateSplitting(
                ",eenie, meenie  ,  , miny,, moe,",
                new String[] {"", "eenie", " meenie  ", "  ", " miny", "", " moe", ""});
    }

    @Test
    public void stringWithFirstAndLastBlankValues()
    {
        this.validateSplitting(" ,eenie, meenie,  ", new String[] {" ", "eenie", " meenie", "  "});
    }

    @Test
    public void stringWithQuotesAndFirstAndLastBlankValues()
    {
        this.validateSplitting(" ,\"eenie\",\" meenie\",  ", new String[] {" ", "\"eenie\"", "\" meenie\"", "  "});
    }

    @Test
    public void simpleStringWithQuotes()
    {
        this.validateSplitting("\"aa\",\"bb\",\"cc\"", new String[] {"\"aa\"", "\"bb\"", "\"cc\""});
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
        MutableList<String> result = Lists.mutable.of();
        this.dataSet.splitMindingQsInto(input, result);

        Assertions.assertArrayEquals(expected, result.toArray());
    }
}
