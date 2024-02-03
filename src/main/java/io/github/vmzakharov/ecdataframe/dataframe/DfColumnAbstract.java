package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

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
        return this.isStored()
                ?
                attachTo.newColumn(newName, this.getType())
                :
                attachTo.newColumn(newName, this.getType(), ((DfColumnComputed) this).getExpressionAsString());
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

        DfColumnStored newColumn = target.newColumn(this.getName(), this.getType());

        newColumn.ensureInitialCapacity(this.getSize() + other.getSize());

        return newColumn;
    }

    protected DfColumn copyColumnSchemaAndEnsureCapacity(DataFrame target)
    {
        DfColumnStored newColumn = target.newColumn(this.getName(), this.getType());

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
