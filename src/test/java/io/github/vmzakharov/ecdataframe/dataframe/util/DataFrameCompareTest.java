package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DECIMAL;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.addMessage;
import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.messageFromKey;

public class DataFrameCompareTest
{
    private DataFrameCompare comparer;

    @Before
    public void setUpComparer()
    {
        this.comparer = new DataFrameCompare();
    }

    @Test
    public void simpleMatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertTrue(new DataFrameCompare().equal(df1, df2));
        Assert.assertTrue(new DataFrameCompare().equal(df2, df1));
        Assert.assertTrue(new DataFrameCompare().equal(df1, df1));
    }

    @Test
    public void simpleMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
                messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                        .with("rowIndex", 1).with("columnIndex", 2).with("lhValue", 102.02).with("rhValue", 555.55)
                        .toString(),
                this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
    }

    @Test
    public void columnHeaderMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Waldo").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        ImmutableList<String> df1Headers = Lists.immutable.of("Foo", "Bar", "Waldo", "Qux");
        ImmutableList<String> df2Headers = Lists.immutable.of("Foo", "Bar", "Baz", "Qux");

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
            messageFromKey("DF_EQ_COL_HEADER_MISMATCH")
                .with("lhColumnHeaders", df1Headers.toString()).with("rhColumnHeaders", df2Headers.toString())
                .toString(),
            this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
        Assert.assertEquals(
            messageFromKey("DF_EQ_COL_HEADER_MISMATCH")
                .with("lhColumnHeaders", df2Headers.toString()).with("rhColumnHeaders", df1Headers.toString())
                .toString(),
            this.comparer.reason()
        );
    }

    @Test
    public void columnTypeMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addStringColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, "101.01", BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, "555.55", BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, "103.03", BigDecimal.valueOf(127, 2))
                ;

        ImmutableList<ValueType> df1Types = Lists.immutable.of(STRING, LONG, DOUBLE, DECIMAL);
        ImmutableList<ValueType> df2Types = Lists.immutable.of(STRING, LONG, STRING, DECIMAL);

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
                messageFromKey("DF_EQ_COL_TYPE_MISMATCH")
                        .with("lhColumnTypes", df1Types.toString()).with("rhColumnTypes", df2Types.toString())
                        .toString(),
                this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
    }

    @Test
    public void dimensionMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice", 10, 101.01)
                .addRow("Carl", 12, 103.03)
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
                messageFromKey("DF_EQ_DIM_MISMATCH")
                        .with("lhRowCount", 3).with("lhColumnCount", 4)
                        .with("rhRowCount", 2).with("rhColumnCount", 3)
                        .toString(),
                this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
    }

    @Test
    public void decimalMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
                messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                        .with("rowIndex", 2).with("columnIndex", 3)
                        .with("lhValue", BigDecimal.valueOf(555, 5).toString()).with("rhValue", BigDecimal.valueOf(127, 2).toString())
                        .toString(),
                this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
    }

    @Test
    public void mismatchWithNulls()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow(null, 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
            messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                    .with("rowIndex", 1).with("columnIndex", 0).with("lhValue", "Bob").with("rhValue", null)
                    .toString(),
            this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
    }

    @Test
    public void decimalMismatchWithNulls()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, null)
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertEquals(
            messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                .with("rowIndex", 0).with("columnIndex", 3)
                .with("lhValue", BigDecimal.valueOf(123, 2).toString()).with("rhValue", null)
                .toString(),
            this.comparer.reason()
        );

        Assert.assertFalse(this.comparer.equal(df2, df1));
        Assert.assertEquals(
                messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                        .with("rowIndex", 0).with("columnIndex", 3)
                        .with("lhValue", null).with("rhValue", BigDecimal.valueOf(123, 2).toString())
                        .toString(),
                this.comparer.reason()
        );
    }

    @Test
    public void simpleMatchIgnoringOrder()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                ;

        Assert.assertFalse(this.comparer.equal(df1, df2));
        Assert.assertFalse(this.comparer.equal(df2, df1));

        Assert.assertTrue(this.comparer.equalIgnoreOrder(df1, df2));
        Assert.assertTrue(this.comparer.equalIgnoreOrder(df2, df1));

        Assert.assertTrue(this.comparer.equalIgnoreOrder(df1, df1));
    }
}
