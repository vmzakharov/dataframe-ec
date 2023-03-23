package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.procedure.Procedure;

public interface DfIterate
{
    void forEach(Procedure<DfCursor> action);
}
