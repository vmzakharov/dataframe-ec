package io.github.vmzakharov.ecdataframe.dataframe;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class DfCursor
{
    private final DataFrame dataFrame;
    private int rowIndex;

    public DfCursor(DataFrame newDataFrame)
    {
        this.dataFrame = newDataFrame;
    }

    public String getString(String columnName)
    {
        return this.dataFrame.getString(columnName, this.rowIndex);
    }

    public long getLong(String columnName)
    {
        return this.dataFrame.getLong(columnName, this.rowIndex);
    }

    public long getInt(String columnName)
    {
        return this.dataFrame.getInt(columnName, this.rowIndex);
    }

    public double getDouble(String columnName)
    {
        return this.dataFrame.getDouble(columnName, this.rowIndex);
    }

    public BigDecimal getDecimal(String columnName)
    {
        return this.dataFrame.getDecimal(columnName, this.rowIndex);
    }

    public LocalDate getDate(String columnName)
    {
        return this.dataFrame.getDate(columnName, this.rowIndex);
    }

    public LocalDateTime getDateTime(String columnName)
    {
        return this.dataFrame.getDateTime(columnName, this.rowIndex);
    }

    public int rowIndex()
    {
        return this.rowIndex;
    }

    public DfCursor rowIndex(int newRowIndex)
    {
        this.rowIndex = newRowIndex;
        return this;
    }
}
