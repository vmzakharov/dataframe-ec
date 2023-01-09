package io.github.vmzakharov.ecdataframe.dataframe;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static io.github.vmzakharov.ecdataframe.util.ErrorReporter.exception;
import static io.github.vmzakharov.ecdataframe.util.ErrorReporter.reportAndThrow;
import static io.github.vmzakharov.ecdataframe.util.ErrorReporter.reportAndThrowIf;

public abstract class DfColumnAbstract
implements DfColumn
{
    final private String name;

    private DataFrame dataFrame;

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
    public DataFrame getDataFrame()
    {
        return this.dataFrame;
    }

    public void setDataFrame(DataFrame newDataFrame)
    {
        if (this.dataFrame != null)
        {
            reportAndThrow("Column '" + this.getName() + "' has already been linked to a data frame");
        }

        this.dataFrame = newDataFrame;
    }

    @Override
    public DfColumn cloneSchemaAndAttachTo(DataFrame attachTo)
    {
        return this.cloneSchemaAndAttachTo(attachTo, this.getName());
    }

    @Override
    public DfColumn cloneSchemaAndAttachTo(DataFrame attachTo, String newName)
    {
        DfColumn clonedColumn;
        try
        {
            Class<?> myClass = this.getClass();
            if (this.isStored())
            {
                Constructor<?> nameConstructor = myClass.getDeclaredConstructor(DataFrame.class, String.class);
                clonedColumn = (DfColumn) nameConstructor.newInstance(attachTo, newName);
            }
            else
            {
                Constructor<?> nameConstructor = myClass.getDeclaredConstructor(DataFrame.class, String.class, String.class);
                clonedColumn = (DfColumn) nameConstructor.newInstance(attachTo, newName, ((DfColumnComputed) this).getExpressionAsString());
            }
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
        {
            throw exception("Failed to instantiate a column from ${name}").with("name", this.getName()).get(e);
        }

        attachTo.addColumn(clonedColumn);

        return clonedColumn;
    }

    protected DfColumn validateAndCreateTargetColumn(DfColumn other, DataFrame target)
    {
        reportAndThrowIf(!this.getType().equals(other.getType()),
                () -> "Attempting to merge columns of different types: "
                        + this.getName() + " (" + this.getType() + ") and " + other.getName() + " (" + other.getType() + ")");

        target.addColumn(this.getName(), this.getType());

        DfColumnStored newColumn = (DfColumnStored) target.getColumnAt(target.columnCount() - 1);

        newColumn.ensureInitialCapacity(this.getSize() + other.getSize());

        return newColumn;
    }
}
