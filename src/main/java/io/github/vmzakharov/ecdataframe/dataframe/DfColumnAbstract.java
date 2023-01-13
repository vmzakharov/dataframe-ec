package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

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
            exceptionByKey("DF_COL_ALREADY_LINKED").with("columnName", this.getName()).fire();
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
            throw exceptionByKey("DF_COL_CLONE_FAILED").with("name", this.getName()).get(e);
        }

        attachTo.addColumn(clonedColumn);

        return clonedColumn;
    }

    protected DfColumn validateAndCreateTargetColumn(DfColumn other, DataFrame target)
    {
        if (!this.getType().equals(other.getType()))
        {
            exceptionByKey("DF_MERGE_COL_DIFF_TYPES")
                    .with("firstColumnName", this.getName())
                    .with("firstColumnType", this.getType())
                    .with("secondColumnName", other.getName())
                    .with("secondColumnType", other.getType())
                    .fire();
        }

        target.addColumn(this.getName(), this.getType());

        DfColumnStored newColumn = (DfColumnStored) target.getColumnAt(target.columnCount() - 1);

        newColumn.ensureInitialCapacity(this.getSize() + other.getSize());

        return newColumn;
    }

    protected DfColumn copyColumnSchema(DataFrame target)
    {
        target.addColumn(this.getName(), this.getType());

        DfColumnStored newColumn = (DfColumnStored) target.getColumnAt(target.columnCount() - 1);

        newColumn.ensureInitialCapacity(this.getSize());

        return newColumn;
    }

    protected void throwAddingIncompatibleValueException(Value value)
    {
        exceptionByKey("DF_BAD_VAL_ADD_TO_COL")
                .with("valueType", value.getType())
                .with("columnName", this.getName())
                .with("columnType", this.getType())
                .with("value", value.asStringLiteral())
                .fire();
    }
}
