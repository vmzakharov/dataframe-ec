package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DfIntColumnTest
{
    private DataFrame df;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.df = new DataFrame("df")
                .addIntColumn("foo").addIntColumn("bar")
                .addRow(1,    2)
                .addRow(2, null)
                .addRow(3,    4)
                .addRow(5,    6)
                ;

        this.df.addColumn("baz", "foo + bar");
        this.df.addColumn("waldo", "bar");
    }

    @Test
    public void computedColumnExpression()
    {
        assertEquals("foo + bar", ((DfColumnComputed) this.df.getColumnNamed("baz")).getExpressionAsString());
        assertEquals("bar", ((DfColumnComputed) this.df.getColumnNamed("waldo")).getExpressionAsString());
    }

    @Test
    public void asIntIterable()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        IntIterable it1 = fooColumn.asIntIterable();
        assertEquals(4, it1.size());
        assertEquals(IntLists.immutable.of(1, 2, 3, 5), it1.toList());

        DfIntColumn barColumn = this.df.getIntColumn("bar");
        IntIterable it2 = barColumn.asIntIterable();
        Exception ex = assertThrows(RuntimeException.class, it2::toList);
        assertEquals("Null value at bar[1]", ex.getMessage());

        DataFrame emptyDf = new DataFrame("empty")
                .addIntColumn("empty");

        DfIntColumn emptyColumn = emptyDf.getIntColumn("empty");
        IntIterable emptyIterable = emptyColumn.asIntIterable();
        assertEquals(0, emptyIterable.size());
        assertEquals(IntLists.immutable.empty(), emptyIterable.toList());
    }

    @Test
    public void addValue()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        fooColumn.addValue(new IntValue(7));
        assertEquals(5, fooColumn.getSize());
        assertEquals(7, fooColumn.getInt(4));

        fooColumn.addValue(Value.VOID);
        assertEquals(6, fooColumn.getSize());
        assertTrue(fooColumn.isNull(5));

        StringValue wrongTypeValue = new StringValue("Hello");
        Exception ex = assertThrows(RuntimeException.class, () -> fooColumn.addValue(wrongTypeValue));

        assertEquals(
            FormatWithPlaceholders.messageFromKey("DF_BAD_VAL_ADD_TO_COL")
               .with("valueType", wrongTypeValue.getType().toString())
               .with("columnName", "foo")
               .with("columnType", fooColumn.getType().toString())
               .with("value", wrongTypeValue.asStringLiteral())
               .toString()
            ,
            ex.getMessage());
    }

    @Test
    public void getInt()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        assertEquals(3, fooColumn.getInt(2));
        assertEquals(5, fooColumn.getInt(3));

        DfIntColumn barColumn = this.df.getIntColumn("bar");
        assertEquals(2, barColumn.getInt(0));

        Exception ex1 = assertThrows(RuntimeException.class, () -> barColumn.getInt(1));
        assertEquals("Null value at bar[1]", ex1.getMessage());

        DfIntColumn waldoColumn = this.df.getIntColumn("waldo");
        assertEquals(2, waldoColumn.getInt(0));

        Exception ex2 = assertThrows(RuntimeException.class, () -> waldoColumn.getInt(1));
        assertEquals("Null value at waldo[1]", ex2.getMessage());
    }

    @Test
    public void getValue()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        assertEquals(new IntValue(3), fooColumn.getValue(2));
        assertEquals(new IntValue(5), fooColumn.getValue(3));

        DfIntColumn barColumn = this.df.getIntColumn("bar");
        assertEquals(new IntValue(2), barColumn.getValue(0));
        assertEquals(Value.VOID, barColumn.getValue(1));

        DfIntColumn waldoColumn = this.df.getIntColumn("waldo");
        assertEquals(new IntValue(2), waldoColumn.getValue(0));
        assertEquals(Value.VOID, waldoColumn.getValue(1));
    }

    @Test
    public void getObject()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        assertEquals(1, fooColumn.getObject(0));
        assertEquals(2, fooColumn.getObject(1));

        DfIntColumn barColumn = this.df.getIntColumn("bar");
        assertEquals(2, barColumn.getObject(0));
        assertNull(barColumn.getObject(1));

        DfLongColumn bazColumn = this.df.getLongColumn("baz");
        assertEquals(3L, bazColumn.getObject(0));
        assertNull(bazColumn.getObject(1));

        DfIntColumn waldoColumn = this.df.getIntColumn("waldo");
        assertEquals(2, waldoColumn.getObject(0));
        assertNull(waldoColumn.getObject(1));
    }

    @Test
    public void setObject()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        fooColumn.setObject(1, 12);
        assertEquals(12, fooColumn.getInt(1));

        fooColumn.setObject(2, null);
        assertNull(fooColumn.getObject(2));
        assertTrue(fooColumn.isNull(2));
    }

    @Test
    public void isNull()
    {
        DfIntColumn fooColumn = this.df.getIntColumn("foo");
        assertFalse(fooColumn.isNull(0));
        assertFalse(fooColumn.isNull(1));

        DfIntColumn barColumn = this.df.getIntColumn("bar");
        assertFalse(barColumn.isNull(0));
        assertTrue(barColumn.isNull(1));

        DfIntColumn waldoColumn = this.df.getIntColumn("waldo");
        assertFalse(waldoColumn.isNull(0));
        assertTrue(waldoColumn.isNull(1));

        DfLongColumn bazColumn = this.df.getLongColumn("baz");
        assertFalse(bazColumn.isNull(0));
        assertTrue(bazColumn.isNull(1));
    }
}
