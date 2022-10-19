package io.github.vmzakharov.ecdataframe.dataframe;

public enum DfColumnSortOrder
{
    ASC, DESC;

    public int order(int comparisonResult)
    {
        return this == DESC ? -comparisonResult : comparisonResult;
    }
}
