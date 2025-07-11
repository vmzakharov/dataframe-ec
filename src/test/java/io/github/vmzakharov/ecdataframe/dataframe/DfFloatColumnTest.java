package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.eclipse.collections.api.FloatIterable;
import org.eclipse.collections.impl.factory.primitive.FloatLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DfFloatColumnTest
{
    public static final double TOLERANCE = 0.000001;
    private DataFrame df;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.df = new DataFrame("df")
                .addFloatColumn("foo").addFloatColumn("bar")
                .addRow(1.0f, 2.0f)
                .addRow(2.0f, null)
                .addRow(3.0f, 4.0f)
                .addRow(5.0f, 6.0f)
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
    public void asFloatIterable()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        FloatIterable it1 = fooColumn.asFloatIterable();
        assertEquals(4, it1.size());
        assertEquals(FloatLists.immutable.of(1.0f, 2.0f, 3.0f, 5.0f), it1.toList());

        DfFloatColumn barColumn = this.df.getFloatColumn("bar");
        FloatIterable it2 = barColumn.asFloatIterable();
        Exception ex = assertThrows(RuntimeException.class, it2::toList);
        assertEquals("Null value at bar[1]", ex.getMessage());

        DataFrame emptyDf = new DataFrame("empty")
                .addFloatColumn("empty");

        DfFloatColumn emptyColumn = emptyDf.getFloatColumn("empty");
        FloatIterable emptyIterable = emptyColumn.asFloatIterable();
        assertEquals(0, emptyIterable.size());
        assertEquals(FloatLists.immutable.empty(), emptyIterable.toList());
    }

    @Test
    public void addValue()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        fooColumn.addValue(new FloatValue(7.0f));
        assertEquals(5, fooColumn.getSize());
        assertEquals(7.0f, fooColumn.getFloat(4), TOLERANCE);

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
    public void getFloat()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        assertEquals(3.0f, fooColumn.getFloat(2), TOLERANCE);
        assertEquals(5.0f, fooColumn.getFloat(3), TOLERANCE);

        DfFloatColumn barColumn = this.df.getFloatColumn("bar");
        assertEquals(2.0f, barColumn.getFloat(0), TOLERANCE);

        Exception ex1 = assertThrows(RuntimeException.class, () -> barColumn.getFloat(1));
        assertEquals("Null value at bar[1]", ex1.getMessage());

        DfFloatColumn waldoColumn = this.df.getFloatColumn("waldo");
        assertEquals(2.0f, waldoColumn.getFloat(0), TOLERANCE);

        Exception ex2 = assertThrows(RuntimeException.class, () -> waldoColumn.getFloat(1));
        assertEquals("Null value at waldo[1]", ex2.getMessage());
    }

    @Test
    public void getValue()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        assertEquals(new FloatValue(3.0f), fooColumn.getValue(2));
        assertEquals(new FloatValue(5.0f), fooColumn.getValue(3));

        DfFloatColumn barColumn = this.df.getFloatColumn("bar");
        assertEquals(new FloatValue(2.0f), barColumn.getValue(0));
        assertEquals(Value.VOID, barColumn.getValue(1));

        DfFloatColumn waldoColumn = this.df.getFloatColumn("waldo");
        assertEquals(new FloatValue(2.0f), waldoColumn.getValue(0));
        assertEquals(Value.VOID, waldoColumn.getValue(1));
    }

    @Test
    public void getObject()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        assertEquals(1.0f, fooColumn.getObject(0));
        assertEquals(2.0f, fooColumn.getObject(1));

        DfFloatColumn barColumn = this.df.getFloatColumn("bar");
        assertEquals(2.0f, barColumn.getObject(0));
        assertNull(barColumn.getObject(1));

        DfDoubleColumn bazColumn = this.df.getDoubleColumn("baz");
        assertEquals(3.0, bazColumn.getObject(0));
        assertNull(bazColumn.getObject(1));

        DfFloatColumn waldoColumn = this.df.getFloatColumn("waldo");
        assertEquals(2.0f, waldoColumn.getObject(0));
        assertNull(waldoColumn.getObject(1));
    }

    @Test
    public void setObject()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        fooColumn.setObject(1, 12.0f);
        assertEquals(12.0f, fooColumn.getFloat(1), TOLERANCE);

        fooColumn.setObject(2, null);
        assertNull(fooColumn.getObject(2));
        assertTrue(fooColumn.isNull(2));
    }

    @Test
    public void isNull()
    {
        DfFloatColumn fooColumn = this.df.getFloatColumn("foo");
        assertFalse(fooColumn.isNull(0));
        assertFalse(fooColumn.isNull(1));

        DfFloatColumn barColumn = this.df.getFloatColumn("bar");
        assertFalse(barColumn.isNull(0));
        assertTrue(barColumn.isNull(1));

        DfFloatColumn waldoColumn = this.df.getFloatColumn("waldo");
        assertFalse(waldoColumn.isNull(0));
        assertTrue(waldoColumn.isNull(1));

        DfDoubleColumn bazColumn = this.df.getDoubleColumn("baz");
        assertFalse(bazColumn.isNull(0));
        assertTrue(bazColumn.isNull(1));
    }
}
