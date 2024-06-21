package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;

import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.text.ParseException;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.INT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.*;

public class FormattedColumnsTest
{
    @Test
    public void loadDecimalFormat()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, "\"$#,##0.0#\";\"($#)\"");
        schema.addColumn("Quantity", LONG, "\"#,###\";\"(#)\"");
        schema.addColumn("Letter", STRING);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema,
                """
                Name,Amount,Quantity,Letter
                "Alice", "$0.12","1,234", "A"
                "Bob",   "$0.0"  ,"(2,345)", "A"
                "Carl",  "$0101.01", "0", "A"
                "Diane", "$100.00", "0", "A"
                "Ed",    "($100.15)",  "000", "A"
                "Frank", "$1,100.15", "(0)",  "A\""""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDoubleColumn("Amount").addLongColumn("Quantity").addStringColumn("Letter")
                        .addRow("Alice",     0.12,  1_234, "A")
                        .addRow("Bob",       0.0,  -2_345, "A")
                        .addRow("Carl",    101.01,      0, "A")
                        .addRow("Diane",   100.0,       0, "A")
                        .addRow("Ed",     -100.15,      0, "A")
                        .addRow("Frank", 1_100.15,      0, "A")
                ,
                loaded
        );
    }

    @Test
    public void loadDecimalFormatInt()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, "\"$#,##0.0#\";\"($#)\"");
        schema.addColumn("Quantity", INT, "\"#,###\";\"(#)\"");
        schema.addColumn("Letter", STRING);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema,
                """
                Name,Amount,Quantity,Letter
                "Alice", "$0.12","1,234", "A"
                "Bob",   "$0.0"  ,"(2,345)", "A"
                "Carl",  "$0101.01", "0", "A"
                "Diane", "$100.00", "0", "A"
                "Ed",    "($100.15)",  "000", "A"
                "Frank", "$1,100.15", "(0)",  "A\""""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDoubleColumn("Amount").addIntColumn("Quantity").addStringColumn("Letter")
                        .addRow("Alice",     0.12,  1_234, "A")
                        .addRow("Bob",       0.0,  -2_345, "A")
                        .addRow("Carl",    101.01,      0, "A")
                        .addRow("Diane",   100.0,       0, "A")
                        .addRow("Ed",     -100.15,      0, "A")
                        .addRow("Frank", 1_100.15,      0, "A")
                ,
                loaded
        );
    }

    @Test
    public void printDecimalFormat()
    {
        DataFrame df = new DataFrame("Frame Of Data")
            .addStringColumn("Name").addDoubleColumn("Amount").addLongColumn("Quantity").addStringColumn("Letter")
            .addRow("Alice",     0.12,  1_234, "A")
            .addRow("Bob",       0.0,  -2_345, "A")
            .addRow("Carl",    101.01,      0, "A")
            .addRow("Diane",   100.0,      -0, "A")
            .addRow("Ed",     -100.15,      0, "A")
            .addRow("Frank", 1_100.15,      0, "A")
            ;

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, "\"$#,##0.00\";\"($#,##0.0#)\"");
        schema.addColumn("Quantity", LONG, "\"#,##0\";\"(#,##0)\"");
        schema.addColumn("Letter", STRING);

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema, "");

        dataSet.write(df);

        assertEquals(
                """
                Name,Amount,Quantity,Letter
                "Alice","$0.12","1,234","A"
                "Bob","$0.00","(2,345)","A"
                "Carl","$101.01","0","A"
                "Diane","$100.00","0","A"
                "Ed","($100.15)","0","A"
                "Frank","$1,100.15","0","A"
                """
                ,
                dataSet.getWrittenData()
        );
    }

    @Test
    public void printDecimalFormatInt()
    {
        DataFrame df = new DataFrame("Frame Of Data")
                .addStringColumn("Name").addDoubleColumn("Amount").addIntColumn("Quantity").addStringColumn("Letter")
                .addRow("Alice",     0.12,  1_234, "A")
                .addRow("Bob",       0.0,  -2_345, "A")
                .addRow("Carl",    101.01,      0, "A")
                .addRow("Diane",   100.0,      -0, "A")
                .addRow("Ed",     -100.15,      0, "A")
                .addRow("Frank", 1_100.15,      0, "A")
                ;

        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, "\"$#,##0.00\";\"($#,##0.0#)\"");
        schema.addColumn("Quantity", INT, "\"#,##0\";\"(#,##0)\"");
        schema.addColumn("Letter", STRING);

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema, "");

        dataSet.write(df);

        assertEquals(
                """
                Name,Amount,Quantity,Letter
                "Alice","$0.12","1,234","A"
                "Bob","$0.00","(2,345)","A"
                "Carl","$101.01","0","A"
                "Diane","$100.00","0","A"
                "Ed","($100.15)","0","A"
                "Frank","$1,100.15","0","A"
                """
                ,
                dataSet.getWrittenData()
        );
    }

    @Test
    public void ignoreNonNumericChars()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, ".");
        schema.addColumn("Quantity", LONG, "0");
        schema.addColumn("Letter", STRING);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema,
                """
                Name,Amount,Quantity,Letter
                "Alice", "    $0.12",   "1,123", "A"
                "Bob",   "     $0.0"  ,  "#123", "A"
                "Carl",  " $0101.01",    " 001", "A"
                "Diane", "  $100.00 ",   "   2", "A"
                "Ed",    "  $100.15",    "3   ", "A"
                "Frank", "$1,100.15",    " 4  ", "A\""""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                .addStringColumn("Name").addDoubleColumn("Amount").addLongColumn("Quantity").addStringColumn("Letter")
                .addRow("Alice",     0.12, 1_123, "A")
                .addRow("Bob",       0.0,    123, "A")
                .addRow("Carl",    101.01,     1, "A")
                .addRow("Diane",   100.0,      2, "A")
                .addRow("Ed",      100.15,     3, "A")
                .addRow("Frank", 1_100.15,     4, "A")
                ,
                loaded
        );
    }

    @Test
    public void ignoreNonNumericCharsCommaDecimalSeparator()
    {
        CsvSchema schema = new CsvSchema();
        schema.addColumn("Name", STRING);
        schema.addColumn("Amount", DOUBLE, ",");
        schema.addColumn("Letter", STRING);

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Numbers", schema,
                """
                Name,Amount,Letter
                "Alice", "    $0,12",   "A"
                "Bob",   "     $0,0"  , "A"
                "Carl",  " $0101,01",   "A"
                """
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                .addStringColumn("Name").addDoubleColumn("Amount").addStringColumn("Letter")
                .addRow("Alice",     0.12, "A")
                .addRow("Bob",       0.0,  "A")
                .addRow("Carl",    101.01, "A")
                ,
                loaded
        );
    }

    @Test
    public void longFormatWithQuoteAndSeparator()
    {
        DecimalFormat format = new DecimalFormat("\"$#,##0\";\"($#,##0)\"");

        this.checkParsedLong(format, "\"$1,123\"", 1123);
        this.checkParsedLong(format, "\"($1,123)\"", -1123);
        this.checkParsedLong(format, "\"$0\"", 0);
        this.checkParsedLong(format, "\"($0)\"", 0);

        assertEquals("\"$1,123\"", format.format(1_123));
        assertEquals("\"($1,123)\"", format.format(-1_123));
    }

    @Test
    public void doubleFormatWithQuotesAndNegativeParens()
    {
        java.text.DecimalFormat format = new DecimalFormat("\"$#,##0.0#\";\"($#,##0.0#)\"");

        this.checkParsedDouble(format, "\"$1,123.45\"", 1123.45);
        this.checkParsedDouble(format, "\"$0.0\"", 0.0);
        this.checkParsedDouble(format, "\"($0.0)\"", 0.0);
        this.checkParsedDouble(format, "\"($1,123.45)\"", -1123.45);

        assertEquals("\"$1,123.45\"", format.format(1123.45));
        assertEquals("\"($1,123.45)\"", format.format(-1123.45));
    }

    private void checkParsedDouble(java.text.DecimalFormat format, String source, double expected)
    {
        try
        {
            assertEquals(expected, format.parse(source).doubleValue(), 0.000001);
        }
        catch (ParseException e)
        {
            throw new RuntimeException("failed to parse '" + source + "'", e);
        }
    }

    private void checkParsedLong(java.text.DecimalFormat format, String source, long expected)
    {
        try
        {
            assertEquals(expected, format.parse(source).longValue());
        }
        catch (ParseException e)
        {
            throw new RuntimeException("failed to parse '" + source + "'", e);
        }
    }
}
