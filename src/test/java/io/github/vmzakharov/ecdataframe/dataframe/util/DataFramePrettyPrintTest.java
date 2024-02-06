package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFramePrettyPrintTest
{
    private DataFrame df;

    @Before
    public void setupDataFrame()
    {
        this.df = new DataFrame("frame")
                .addStringColumn("Foo").addLongColumn("Bar").addStringColumn("Baz").addDoubleColumn("Waldo")
                .addRow("Alice", 1_000, "abc", 12.3456)
                .addRow("Bob", 20, "def hij", 112.34)
                .addRow("Carl", 3, "xyz", 1_234.345678)
                .addRow("Doris", 4_000_000, "klm", 1.3)
        ;
    }

    @Test
    public void simple()
    {
        String expected =
                  "|Foo    |    Bar|Baz      |    Waldo|\n"
                + "|\"Alice\"|   1000|\"abc\"    |  12.3456|\n"
                + "|\"Bob\"  |     20|\"def hij\"| 112.3400|\n"
                + "|\"Carl\" |      3|\"xyz\"    |1234.3457|\n"
                + "|\"Doris\"|4000000|\"klm\"    |   1.3000|\n";

        Assert.assertEquals(expected, new DataFramePrettyPrint().prettyPrint(this.df));
    }

    @Test
    public void overrideFormats()
    {
        String expected =
                   "|Foo  |      Bar|Baz    |      Waldo|\n"
                 + "|Alice|    1,000|abc    |   12.34560|\n"
                 + "|Bob  |       20|def hij|  112.34000|\n"
                 + "|Carl |        3|xyz    |1,234.34568|\n"
                 + "|Doris|4,000,000|klm    |    1.30000|\n";

        Assert.assertEquals(expected,
                new DataFramePrettyPrint()
                    .doubleFormat("%,.5f")
                    .longFormat("%,d")
                    .quoteStrings(false)
                    .prettyPrint(this.df));
    }

    @Test
    public void horizontalSeparators()
    {
        String expected =
                  "+-----+-------+-------+---------+\n"
                + "|Foo  |    Bar|Baz    |    Waldo|\n"
                + "+-----+-------+-------+---------+\n"
                + "|Alice|   1000|abc    |  12.3456|\n"
                + "+-----+-------+-------+---------+\n"
                + "|Bob  |     20|def hij| 112.3400|\n"
                + "+-----+-------+-------+---------+\n"
                + "|Carl |      3|xyz    |1234.3457|\n"
                + "+-----+-------+-------+---------+\n"
                + "|Doris|4000000|klm    |   1.3000|\n"
                + "+-----+-------+-------+---------+\n";

        String formatted = new DataFramePrettyPrint()
                .quoteStrings(false)
                .horizontalSeparators(true)
                .prettyPrint(this.df);

        Assert.assertEquals(expected, formatted);
    }

    @Test
    public void longString()
    {
        DataFrame dfWithLongStrings = new DataFrame("frame")
                .addStringColumn("Foo").addLongColumn("This is a very long column header").addStringColumn("Baz").addDoubleColumn("Waldo")
                .addRow("Alice", 1_000, "abc", 12.3456)
                .addRow("A very long string is here, entirely too long", 20, "def hij", 112.34)
                .addRow("Carl", 3, "xyz", 1_234.345678)
                .addRow("Doris", 4_000_000, "This is another long string in this data frame", 1.3)
                ;

        String result = new DataFramePrettyPrint().prettyPrint(dfWithLongStrings);

        String expected =
                  "|Foo                   |This is a very lo...|Baz                   |    Waldo|\n"
                + "|\"Alice\"               |                1000|\"abc\"                 |  12.3456|\n"
                + "|\"A very long strin...\"|                  20|\"def hij\"             | 112.3400|\n"
                + "|\"Carl\"                |                   3|\"xyz\"                 |1234.3457|\n"
                + "|\"Doris\"               |             4000000|\"This is another l...\"|   1.3000|\n";

        Assert.assertEquals(expected, result);
    }

    @Test
    public void longStringOverride()
    {
        String result = new DataFramePrettyPrint()
                .maxStringLength(7)
                .quoteStrings(false)
                .horizontalSeparators(true)
                .prettyPrint(this.df);

        String expected =
                   "+-----+-------+-------+-------+\n"
                 + "|Foo  |    Bar|Baz    |  Waldo|\n"
                 + "+-----+-------+-------+-------+\n"
                 + "|Alice|   1000|abc    |12.3456|\n"
                 + "+-----+-------+-------+-------+\n"
                 + "|Bob  |     20|def hij|112....|\n"
                 + "+-----+-------+-------+-------+\n"
                 + "|Carl |      3|xyz    |1234...|\n"
                 + "+-----+-------+-------+-------+\n"
                 + "|Doris|4000000|klm    | 1.3000|\n"
                 + "+-----+-------+-------+-------+\n";

        Assert.assertEquals(expected, result);
    }
}
