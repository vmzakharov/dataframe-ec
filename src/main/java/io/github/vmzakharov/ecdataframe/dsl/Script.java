package io.github.vmzakharov.ecdataframe.dsl;

import org.eclipse.collections.api.list.ListIterable;

/**
 *  A script is a sequence of one or more statements or expressions. The result of executing a script is the value of
 *  the last statement (or the last standalone expression) that was executed in the script.
 */
public interface Script
extends Expression
{
    /**
     * adds an expression to the script
     * @param anExpression an expression to be added to the script
     * @return the expression
     */
    Expression addExpression(Expression anExpression);

    /**
     * returns all the expressions from the script
     * @return a list of all the expressions in the script
     */
    ListIterable<Expression> getExpressions();
}
