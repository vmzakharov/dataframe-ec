package org.modelscript.dataframe;

public interface DfColumnStored
extends DfColumn
{
    @Override
    default boolean isStored() { return true; }
}
