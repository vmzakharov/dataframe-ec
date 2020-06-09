package org.modelscript.expr;

import org.eclipse.collections.api.list.ListIterable;

public interface FunctionDescriptor
{
    String getName();

    ListIterable<String> getParameterNames();
}
