package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

abstract public class HierarchicalDataSet
extends DataSetAbstract
{
    public HierarchicalDataSet(String newName)
    {
        super(newName);
    }

    abstract public Object getValue(String propertyChainString);

    abstract public ValueType getFieldType(String fieldName);
}
