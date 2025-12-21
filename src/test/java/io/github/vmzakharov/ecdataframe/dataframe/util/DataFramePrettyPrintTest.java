package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

public class DataFramePrettyPrintTest
{
    private DataFrame df;

    @BeforeEach
    public void setupDataFrame()
    {
        this.df = new DataFrame("frame")
                .addStringColumn("Foo").addLongColumn("Bar").addStringColumn("Baz").addDoubleColumn("Waldo")
                .addRow("Alice", 1_000, "abc", 12.3456)
                .addRow("Bob", 20, "def hij", 112.34)
                .addRow("Carl", 3, "xyz", 1_234.345678)
                .addRow("Doris", 4_000_000, "klm", 1.3)
        ;

        Locale.setDefault(Locale.ROOT); // avoids failing tests due to locale differences
    }

    @Test
    public void simple()
    {
        String expected =
                """
                |Foo    |    Bar|Baz      |    Waldo|
                |"Alice"|   1000|"abc"    |  12.3456|
                |"Bob"  |     20|"def hij"| 112.3400|
                |"Carl" |      3|"xyz"    |1234.3457|
                |"Doris"|4000000|"klm"    |   1.3000|
                """;

        assertEquals(expected, new DataFramePrettyPrint().prettyPrint(this.df));
    }

    @Test
    public void overrideFormats()
    {
        String expected =
                """
                |Foo  |      Bar|Baz    |      Waldo|
                |Alice|    1,000|abc    |   12.34560|
                |Bob  |       20|def hij|  112.34000|
                |Carl |        3|xyz    |1,234.34568|
                |Doris|4,000,000|klm    |    1.30000|
                """;

        assertEquals(expected,
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
                """
                +-----+-------+-------+---------+
                |Foo  |    Bar|Baz    |    Waldo|
                +-----+-------+-------+---------+
                |Alice|   1000|abc    |  12.3456|
                +-----+-------+-------+---------+
                |Bob  |     20|def hij| 112.3400|
                +-----+-------+-------+---------+
                |Carl |      3|xyz    |1234.3457|
                +-----+-------+-------+---------+
                |Doris|4000000|klm    |   1.3000|
                +-----+-------+-------+---------+
                """;

        String formatted = new DataFramePrettyPrint()
                .quoteStrings(false)
                .horizontalSeparators(true)
                .prettyPrint(this.df);

        assertEquals(expected, formatted);
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
                """
                |Foo                   |This is a very lo...|Baz                   |    Waldo|
                |"Alice"               |                1000|"abc"                 |  12.3456|
                |"A very long strin..."|                  20|"def hij"             | 112.3400|
                |"Carl"                |                   3|"xyz"                 |1234.3457|
                |"Doris"               |             4000000|"This is another l..."|   1.3000|
                """;

        assertEquals(expected, result);
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
                """
                +-----+-------+-------+-------+
                |Foo  |    Bar|Baz    |  Waldo|
                +-----+-------+-------+-------+
                |Alice|   1000|abc    |12.3456|
                +-----+-------+-------+-------+
                |Bob  |     20|def hij|112....|
                +-----+-------+-------+-------+
                |Carl |      3|xyz    |1234...|
                +-----+-------+-------+-------+
                |Doris|4000000|klm    | 1.3000|
                +-----+-------+-------+-------+
                """;

        assertEquals(expected, result);
    }
}
