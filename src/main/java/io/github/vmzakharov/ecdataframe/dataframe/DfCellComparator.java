package io.github.vmzakharov.ecdataframe.dataframe;

public interface DfCellComparator
{
    ComparisonResult compare(int thisRowIndex, int otherRowIndex);
}
