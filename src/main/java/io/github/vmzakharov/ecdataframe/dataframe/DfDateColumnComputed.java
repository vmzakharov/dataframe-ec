package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;

import java.time.LocalDate;

public class DfDateColumnComputed
extends DfObjectColumnComputed<LocalDate>
implements DfDateColumn
{
    public DfDateColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public LocalDate getTypedObject(int rowIndex)
    {
        return this.getDate(rowIndex);
    }

    @Override
    public LocalDate getDate(int rowIndex)
    {
        // todo: column in the variable expr or some other optimization?
        DataFrameEvalContext evalContext = this.getDataFrame().getEvalContext();
        evalContext.setRowIndex(rowIndex);

        DateValue result = (DateValue) this.getExpression().evaluate(new InMemoryEvaluationVisitor(evalContext));
        return result.dateValue();
    }
}
