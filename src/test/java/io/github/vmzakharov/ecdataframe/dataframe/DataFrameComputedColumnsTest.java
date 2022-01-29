package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

public class DataFrameComputedColumnsTest
{
    private DataFrame df;

    @Before
    public void initializeDataFrame()
    {
        this.df = new DataFrame("this.df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice",  5, 23.45)
                .addRow("Bob",   10, 12.34)
                .addRow("Carol", 11, 56.78)
                .addRow("Dan",    0,  7.89);
    }

    @Test
    public void constantComputedColumn()
    {
        this.df.addLongColumn("Number Two", "2");

        Assert.assertEquals(4, this.df.columnCount());
        Assert.assertEquals(4, this.df.rowCount());

        for (int i = 0; i < 4; i++)
        {
            Assert.assertEquals(2, this.df.getLong("Number Two", i));
        }
    }

    @Test
    public void simpleColumnDependentExpression()
    {
        this.df.addLongColumn("Double Count", "Count * 2").addLongColumn("Two More", "Count + 2");

        Assert.assertEquals(5, this.df.columnCount());
        Assert.assertEquals(4, this.df.rowCount());

        Assert.assertEquals(LongLists.immutable.of(10L, 20L, 22L, 0L), this.df.getLongColumn("Double Count").toLongList());
        Assert.assertEquals(7L, this.df.getLongColumn("Two More").getLong(0));
        Assert.assertEquals(12L, this.df.getLongColumn("Two More").getLong(1));
        Assert.assertEquals(13L, this.df.getLongColumn("Two More").getLong(2));
        Assert.assertEquals(2L, this.df.getLongColumn("Two More").getLong(3));
    }

    @Test
    public void computedColumnDependentExpression()
    {
        this.df
            .addLongColumn("CountTwiceAndTwo", "CountTwice + 2")
            .addLongColumn("AllTogetherNow", "CountTwice + CountTwiceAndTwo")
            .addLongColumn("CountTwice", "Count * 2");

        Assert.assertEquals(LongLists.immutable.of(12L, 22L, 24L, 2L), this.df.getLongColumn("CountTwiceAndTwo").toLongList());
        Assert.assertEquals(LongLists.immutable.of(22L, 42L, 46L, 2L), this.df.getLongColumn("AllTogetherNow").toLongList());
    }

    @Test
    public void externalEvalContext()
    {
        SimpleEvalContext evalContext = new SimpleEvalContext();
        evalContext.setVariable("Three", new LongValue(3));

        this.df.addLongColumn("CountMore", "Count * Three");

        this.df.setExternalEvalContext(evalContext);

        Assert.assertEquals(LongLists.immutable.of(15L, 30L, 33L, 0L), this.df.getLongColumn("CountMore").toLongList());
    }
    
    @Test
    public void doubleComputedColumn()
    {
        this.df.addDoubleColumn("ValuePlus", "Value + 1.01");

        Assert.assertEquals(DoubleLists.immutable.of(24.46, 13.35, 57.79, 8.90), this.df.getDoubleColumn("ValuePlus").toDoubleList());
    }

    @Test
    public void stringComputedColumnSimple()
    {
        this.df.addStringColumn("Greeting", "\"Dear \" + Name");

        Assert.assertEquals(Lists.immutable.of("Dear Alice", "Dear Bob", "Dear Carol", "Dear Dan"), this.df.getStringColumn("Greeting").toList());
    }

    @Test
    public void stringComputedColumnFunction()
    {
        this.df.addStringColumn("Nickname", "substr(Name, 0, 3) + \"ka\"");

        Assert.assertEquals(Lists.immutable.of("Alika", "Bobka", "Carka", "Danka"), this.df.getStringColumn("Nickname").toList());
    }

    @Test
    public void computedColumnUsingSimpleScript()
    {
        this.df = new DataFrame("this.df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");

        this.df
                .addRow("Alice",  5, 23.45)
                .addRow("Bob",   10, 12.34)
                .addRow("Carol", 11, 56.78)
                .addRow("Dan",    0,  7.89);

        String script =
                  "if Count > 7 then\n"
                + "  Value * 2\n"
                + "else\n"
                + "  Value + 2\n"
                + "endif";

        this.df.addDoubleColumn("Complication", script);

        Assert.assertEquals(DoubleLists.immutable.of(25.45, 24.68, 113.56, 9.89), this.df.getDoubleColumn("Complication").toDoubleList());
    }

    @Test
    public void usingScriptWithVariables()
    {
        String script =
                  "a = 7\n"
                + "if Count > a then\n"
                + "  Value * 2\n"
                + "else\n"
                + "  Value + 2\n"
                + "endif";
        this.df.addDoubleColumn("Complication", script);

        Assert.assertEquals(DoubleLists.immutable.of(25.45, 24.68, 113.56, 9.89), this.df.getDoubleColumn("Complication").toDoubleList());
    }

    @Test
    public void usingScriptWithFunctionDeclaration()
    {
        String script =
                  "function bigEnough(value)\n"
                + "{\n"
                + "   value > 7\n"
                + "}\n"
                + "\n"
                + "if bigEnough(Count) then\n"
                + "  Value * 2\n"
                + "else\n"
                + "  Value + 2\n"
                + "endif";

        this.df.addDoubleColumn("Complication", script);

        Assert.assertEquals(DoubleLists.immutable.of(25.45, 24.68, 113.56, 9.89), this.df.getDoubleColumn("Complication").toDoubleList());
    }

    @Test
    public void usingScriptWithOutsideContext()
    {
        String script =
                  "if inTheList(Count, numbers) then\n"
                + "  Value * 2\n"
                + "else\n"
                + "  Value + 2\n"
                + "endif";

        this.df.addDoubleColumn("Complication", script);

        String outerScriptString =
                  "function inTheList(item, list)\n"
                + "{\n"
                + "  item in list\n"
                + "}\n"
        ;

        AnonymousScript outerScript = ExpressionParserHelper.DEFAULT.toScript(outerScriptString);

        SimpleEvalContext outerContext = new SimpleEvalContext();
        outerContext.setDeclaredFunctions(outerScript.getFunctions());
        outerContext.setVariable("numbers", ExpressionTestUtil.evaluate("(5, 10)"));

        this.df.getEvalContext().setNestedContext(outerContext);

        Assert.assertEquals(DoubleLists.immutable.of(46.9, 24.68, 58.78, 9.89), this.df.getDoubleColumn("Complication").toDoubleList());
    }

    @Test
    public void computedDateColumn()
    {
        DataFrame dataFrame  = new DataFrame("DataFrame")
                .addStringColumn("DateAsString").addLongColumn("Year").addLongColumn("Month").addLongColumn("Day")
                .addRow("2021-10-01", 2021, 11, 22)
                .addRow("2022-10-02", 2022, 11, 23)
                .addRow("2023-10-03", 2023, 11, 24)
                ;

        dataFrame.addDateColumn("fromString", "toDate(DateAsString)");
        dataFrame.addDateColumn("fromNumbers", "toDate(Year, Month, Day)");

        DataFrameUtil.assertEquals(
                new DataFrame("DataFrame")
                        .addStringColumn("DateAsString").addLongColumn("Year").addLongColumn("Month").addLongColumn("Day")
                        .addDateColumn("fromString").addDateColumn("fromNumbers")
                        .addRow("2021-10-01", 2021, 11, 22, LocalDate.of(2021, 10, 1), LocalDate.of(2021, 11, 22))
                        .addRow("2022-10-02", 2022, 11, 23, LocalDate.of(2022, 10, 2), LocalDate.of(2022, 11, 23))
                        .addRow("2023-10-03", 2023, 11, 24, LocalDate.of(2023, 10, 3), LocalDate.of(2023, 11, 24))
                , dataFrame
        );
    }
}
