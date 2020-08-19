package org.modelscript.dataframe;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public abstract class DfColumnAbstract
implements DfColumn
{
    final private String name;

    private DataFrame dataFrame;

    public DfColumnAbstract(String newName)
    {
        this.name = newName;
    }

    public DfColumnAbstract(DataFrame newDataFrame, String newName)
    {
        this.dataFrame = newDataFrame;
        this.name = newName;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public String getValueAsStringLiteral(int rowIndex)
    {
        return this.getValueAsString(rowIndex);
    }

    @Override
    public DataFrame getDataFrame()
    {
        return this.dataFrame;
    }

    public void setDataFrame(DataFrame newDataFrame)
    {
        if (this.dataFrame != null)
        {
            throw new RuntimeException("Column '" + this.getName() + "' has already been linked to a data frame");
        }
        this.dataFrame = newDataFrame;
    }

    @Override
    public DfColumn cloneSchemaAndAttachTo(DataFrame attachTo)
    {
        DfColumn clonedColumn;
        try
        {
            Class myClass = this.getClass();
            if (this.isStored())
            {
                Constructor nameConstructor = myClass.getDeclaredConstructor(DataFrame.class, String.class);
                clonedColumn = (DfColumn) nameConstructor.newInstance(attachTo, this.getName());
            }
            else
            {
                Constructor nameConstructor = myClass.getDeclaredConstructor(DataFrame.class, String.class, String.class);
                clonedColumn = (DfColumn) nameConstructor.newInstance(attachTo, this.getName(), ((DfColumnComputed) this).getExpressionAsString());
            }
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
        {
            throw new RuntimeException("Failed to instantiate a column from " + this.getName(), e);
        }

        attachTo.addColumn(clonedColumn);

        return clonedColumn;
    }
}
