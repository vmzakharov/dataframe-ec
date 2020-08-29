package io.github.vmzakharov.ecdataframe.dsl;

import org.eclipse.collections.api.list.ListIterable;

public interface FunctionDescriptor
{
    String getName();

    ListIterable<String> getParameterNames();
}
