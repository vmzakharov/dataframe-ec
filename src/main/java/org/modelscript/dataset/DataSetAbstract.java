package org.modelscript.dataset;

abstract public class DataSetAbstract
implements DataSet
{
    private final String name;

    public DataSetAbstract(String newName)
    {
        this.name = newName;
    }

    @Override
    public String getName()
    {
        return this.name;
    }
}
