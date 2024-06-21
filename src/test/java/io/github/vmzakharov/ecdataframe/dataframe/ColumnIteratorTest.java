package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ColumnIteratorTest
{
    private DataFrame dataFrame;

    @BeforeEach
    public void setupDataFrame()
    {
        this.dataFrame = new DataFrame("Test")
                .addStringColumn("Name").addStringColumn("Quux").addStringColumn("Waldo")
                .addRow("Alice", "A", "A")
                .addRow("Bob", "B", "A")
                .addRow("Carl", "C", null)
                .addRow("Doris", "D", "B")
                ;
    }

    @Test
    public void injectInto()
    {
        int totalStringLength = this.dataFrame.getStringColumn("Name")
            .injectIntoBreakOnNulls(
                    0,
                    (len, s) -> len + s.length()
            );

        assertEquals(17, totalStringLength);
    }

    @Test
    public void injectIntoWithBreakOnNullResult()
    {
        Integer totalStringLength = this.dataFrame.getStringColumn("Quux")
            .injectIntoBreakOnNulls(
                    0,
                    (len, s) -> {
                        if (s.equals("C"))
                        {
                            fail("Shouldn't reach C");
                        }

                        return s.equals("B") ? null : len + s.length();
                    }
            );

        assertNull(totalStringLength);
    }

    @Test
    public void injectIntoWithBreakOnNullValue()
    {
        Integer totalStringLength = this.dataFrame.getStringColumn("Waldo")
            .injectIntoBreakOnNulls(
                    0,
                    (len, s) -> {
                        if (!s.equals("A"))
                        {
                            fail("Shouldn't get here");
                        }

                        return len + s.length();
                    }
            );

        assertNull(totalStringLength);
    }
}
