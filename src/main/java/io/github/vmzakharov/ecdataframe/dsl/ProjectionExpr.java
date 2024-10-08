package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.PrettyPrintVisitor;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

public record ProjectionExpr(
        ListIterable<Expression> projectionElements,
        Expression whereClause,
        ListIterable<String> elementNames,
        ListIterable<Expression> projectionExpressions)
implements Expression
{
    public ProjectionExpr(ListIterable<Expression> newProjectionElements)
    {
        this(newProjectionElements, null);
    }

    public ProjectionExpr(ListIterable<Expression> projectionElements, Expression whereClause)
    {
        this(projectionElements, whereClause, Lists.mutable.empty(), Lists.mutable.empty());

        // need to update these properties but don't want to expose the mutable type in the record API
        // so the property is declared as "Iterable", which means we have to cast here to update it
        MutableList<String> mElementNames = (MutableList<String>) this.elementNames;
        MutableList<Expression> mProjectionExpressions = (MutableList<Expression>) this.projectionExpressions;

        for (int i = 0; i < this.projectionElements.size(); i++)
        {
            Expression element = this.projectionElements.get(i);
            if (element instanceof AliasExpr aliasExpr)
            {
                mElementNames.add(aliasExpr.alias());
                mProjectionExpressions.add(aliasExpr.expression());
            }
            else
            {
                mElementNames.add(PrettyPrintVisitor.exprToString(element));
                mProjectionExpressions.add(element);
            }
        }
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitProjectionExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitProjectionExpr(this);
    }
}
