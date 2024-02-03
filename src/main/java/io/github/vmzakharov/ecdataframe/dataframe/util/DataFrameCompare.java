package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.ConfigureMessages;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.eclipse.collections.api.list.ImmutableList;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.messageFromKey;

/**
 * A utility class used to compare two data frames.
 */
public class DataFrameCompare
{
    private String reason = "";

    static
    {
        ConfigureMessages.initialize();
    }

    public DataFrameCompare()
    {
    }

    /**
     * Compares two data frames. The data frames are considered equal if they have the same dimensions, the same column
     * types and headers and cells in the same positions contain equal values.
     * @param thisDf the first data frame to be compared
     * @param thatDf the second data frame to be compared
     * @param tolerance the tolerance to be used when comparing double cell values
     * @return true if the data frames are equal, false otherwise
     */
    public boolean equal(DataFrame thisDf, DataFrame thatDf, double tolerance)
    {
        if (this.dimensionsDoNotMatch(thisDf, thatDf)
            || this.headersDoNotMatch(thisDf, thatDf)
            || this.columnTypesDoNotMatch(thisDf, thatDf))
        {
            return false;
        }

        return this.cellValuesMatch(thisDf, thatDf, tolerance);
    }

    /**
     * Compares two data frames. The data frames are considered equal if they have the same dimensions, the same column
     * types and headers and cells in the same positions contain equal values.
     * @param thisDf the first data frame to be compared
     * @param thatDf the second data frame to be compared
     * @return true if the data frames are equal, false otherwise
     */
    public boolean equal(DataFrame thisDf, DataFrame thatDf)
    {
        return this.equal(thisDf, thatDf, 0.0);
    }

    /**
     * Compares two data frames ignoring the order of rows. The data frames are sorted and then compared.
     * Note: this method has a side effect of sorting dataframes.
     * @param thisDf the first data frame to be compared
     * @param thatDf the second data frame to be compared
     * @return true if the data frames are equal, false otherwise
     */
    public boolean equalIgnoreOrder(DataFrame thisDf, DataFrame thatDf)
    {
        return this.equalIgnoreOrder(thisDf, thatDf, 0.0);
    }

    /**
     * Compares two data frames ignoring the order of rows. The data frames are sorted and then compared.
     * Note: this method has a side effect of sorting dataframes.
     * @param thisDf the first data frame to be compared
     * @param thatDf the second data frame to be compared
     * @return true if the data frames are equal, false otherwise
     */
    public boolean equalIgnoreOrder(DataFrame thisDf, DataFrame thatDf, double tolerance)
    {
        if (this.dimensionsDoNotMatch(thisDf, thatDf)
                || this.headersDoNotMatch(thisDf, thatDf)
                || this.columnTypesDoNotMatch(thisDf, thatDf))
        {
            return false;
        }

        return this.cellValuesMatch(
                thisDf.sortBy(thisDf.getColumns().collect(DfColumn::getName)),
                thatDf.sortBy(thisDf.getColumns().collect(DfColumn::getName)),
                tolerance
        );
    }

    private boolean cellValuesMatch(DataFrame thisDf, DataFrame thatDf, double tolerance)
    {
        int colCount = thisDf.columnCount();
        int rowCount = thisDf.rowCount();

        boolean failed = false;

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            for (int colIndex = 0; colIndex < colCount; colIndex++)
            {
                Object thisValue = thisDf.getObject(rowIndex, colIndex);
                Object thatValue = thatDf.getObject(rowIndex, colIndex);

                if ((thisValue instanceof BigDecimal) && (thatValue != null))
                {
                    failed = !(thatValue instanceof BigDecimal) || (((BigDecimal) thisValue).compareTo((BigDecimal) thatValue) != 0);
                }
                else if (thisValue instanceof Double && thatValue instanceof Double)
                {
                    double thisDouble = (Double) thisValue;
                    double thatDouble = (Double) thatValue;

                    failed = tolerance == 0.0
                            ? (Double.compare(thisDouble, thatDouble) != 0) : (Math.abs(thisDouble - thatDouble) > tolerance);
                }
                else
                {
                    failed = (thisValue == null && thatValue != null) || (thisValue != null && !thisValue.equals(thatValue));
                }

                if (failed)
                {
                    this.reason(messageFromKey("DF_EQ_CELL_VALUE_MISMATCH")
                            .with("rowIndex", rowIndex).with("columnIndex", colIndex)
                            .with("lhValue", String.valueOf(thisValue)).with("rhValue", String.valueOf(thatValue))
                    );
                    return false;
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

        this.reason(messageFromKey("DF_EQ_COL_TYPE_MISMATCH")
                .with("lhColumnTypes", thisColumnTypes.toString()).with("rhColumnTypes", thatColumnTypes.toString()));
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

        this.reason(messageFromKey("DF_EQ_COL_HEADER_MISMATCH")
            .with("lhColumnHeaders", thisColumnNames.toString()).with("rhColumnHeaders", thatColumnNames.toString()));
        return true;
    }

    private boolean dimensionsDoNotMatch(DataFrame thisDf, DataFrame thatDf)
    {
        if (thisDf.rowCount() == thatDf.rowCount() && thisDf.columnCount() == thatDf.columnCount())
        {
            return false;
        }

        this.reason(messageFromKey("DF_EQ_DIM_MISMATCH")
                        .with("lhRowCount", thisDf.rowCount()).with("lhColumnCount", thisDf.columnCount())
                        .with("rhRowCount", thatDf.rowCount()).with("rhColumnCount", thatDf.columnCount()));
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

    private void reason(FormatWithPlaceholders newReasonFormat)
    {
        this.reason = newReasonFormat.toString();
    }
}
