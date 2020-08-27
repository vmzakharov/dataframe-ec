package io.github.vmzakharov.ecdataframe.expr;

import org.eclipse.collections.api.list.ListIterable;

public interface FunctionDescriptor
{
    String getName();

    ListIterable<String> getParameterNames();
}
