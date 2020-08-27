package io.github.vmzakharov.ecdataframe.dataframe;

public interface DfColumnStored
extends DfColumn
{
    @Override
    default boolean isStored() { return true; }
}
