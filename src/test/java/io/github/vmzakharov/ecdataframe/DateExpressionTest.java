package io.github.vmzakharov.ecdataframe;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DateExpressionTest
{
    @Test
    public void parseDate()
    {
        LocalDate date = ExpressionTestUtil.evaluateToDate("toDate(\"2020-02-15\")");
        assertEquals(LocalDate.of(2020, 2, 15), date);
    }

    @Test
    public void dateProximity()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("withinDays(toDate(\"2020-02-15\"), toDate(\"2020-02-17\"), 3)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("withinDays(toDate(\"2020-02-11\"), toDate(\"2020-02-17\"), 3)"));
    }
}
