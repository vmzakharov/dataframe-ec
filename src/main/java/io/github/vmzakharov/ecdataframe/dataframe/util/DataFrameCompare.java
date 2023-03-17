package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ImmutableList;

import java.math.BigDecimal;

/**
 * A utility class used to compare two data frames.
 */
public class DataFrameCompare
{
    private String reason = "";

    public DataFrameCompare()
    {
    }

    public boolean equal(DataFrame thisDf, DataFrame thatDf)
    {
        if (this.dimensionsDoNotMatch(thisDf, thatDf)
            || this.headersDoNotMatch(thisDf, thatDf)
            || this.columnTypesDoNotMatch(thisDf, thatDf))
        {
            return false;
        }

        return this.cellValuesMatch(thisDf, thatDf);
    }

    public boolean equalIgnoreOrder(DataFrame thisDf, DataFrame thatDf)
    {
        if (this.dimensionsDoNotMatch(thisDf, thatDf)
                || this.headersDoNotMatch(thisDf, thatDf)
                || this.columnTypesDoNotMatch(thisDf, thatDf))
        {
            return false;
        }

        return this.cellValuesMatch(
                thisDf.sortBy(thisDf.getColumns().collect(DfColumn::getName)),
                thatDf.sortBy(thisDf.getColumns().collect(DfColumn::getName))
        );
    }

    private boolean cellValuesMatch(DataFrame thisDf, DataFrame thatDf)
    {
        int colCount = thisDf.columnCount();
        int rowCount = thisDf.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            for (int colIndex = 0; colIndex < colCount; colIndex++)
            {
                Object thisValue = thisDf.getObject(rowIndex, colIndex);
                Object thatValue = thatDf.getObject(rowIndex, colIndex);

                if ((thisValue instanceof BigDecimal) && (thatValue != null))
                {
                    if (!(thatValue instanceof BigDecimal))
                    {
                        this.reason("Invalid type of the right hand value: " + thatValue.getClass().getName()
                                + ", expected: BigDecimal");
                        return false;
                    }

                    if (((BigDecimal) thisValue).compareTo((BigDecimal) thatValue) != 0)
                    {
                        this.reason("Different values in row " + rowIndex + ", column " + colIndex
                                + " lh: " + thisValue + ", rh: " + thatValue
                                + " (showing the result of left hand.compareTo(right hand)");
                        return false;
                    }
                }
                else
                {
                    if ((thisValue == null && thatValue != null)
                            || (thisValue != null && !thisValue.equals(thatValue)))
                    {
                        this.reason("Different values in row " + rowIndex + ", column " + colIndex + ": " + thisValue
                                + " vs. " + thatValue);
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private boolean columnTypesDoNotMatch(DataFrame thisDf, DataFrame thatDf)
    {
        ImmutableList<ValueType> thisColumnTypes = thisDf.getColumns().collect(DfColumn::getType);
        ImmutableList<ValueType> thatColumnTypes = thatDf.getColumns().collect(DfColumn::getType);

        if (thisColumnTypes.equals(thatColumnTypes))
        {
            return false;
        }

        this.reason = "Column types " + thisColumnTypes + " vs. " + thatColumnTypes;
        return true;
    }

    private boolean headersDoNotMatch(DataFrame thisDf, DataFrame thatDf)
    {
        ImmutableList<String> thisColumnNames = thisDf.getColumns().collect(DfColumn::getName);
        ImmutableList<String> thatColumnNames = thatDf.getColumns().collect(DfColumn::getName);

        if (thisColumnNames.equals(thatColumnNames))
        {
            return false;
        }

        this.reason("Column names " + thisColumnNames + " vs. " + thatColumnNames);
        return true;
    }

    private boolean dimensionsDoNotMatch(DataFrame thisDf, DataFrame thatDf)
    {
        if (thisDf.rowCount() == thatDf.rowCount() && thisDf.columnCount() == thatDf.columnCount())
        {
            return false;
        }

        this.reason("Dimensions don't match: rows " + thisDf.rowCount() + ", cols " + thisDf.columnCount()
                + ", vs. rows " + thatDf.rowCount() + ", cols " + thatDf.columnCount());
        return true;
    }

    public String reason()
    {
        return this.reason;
    }

    private void reason(String newReason)
    {
        this.reason = newReason;
    }
}
