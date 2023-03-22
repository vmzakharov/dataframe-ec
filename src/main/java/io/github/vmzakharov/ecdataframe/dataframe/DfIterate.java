package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.procedure.Procedure;

public interface DfIterate
{
    public void forEach(Procedure<DfCursor> action);
}
