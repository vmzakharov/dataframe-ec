package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult;

public interface DfCellComparator
{
    ComparisonResult compare(int thisRowIndex, int otherRowIndex);
}
