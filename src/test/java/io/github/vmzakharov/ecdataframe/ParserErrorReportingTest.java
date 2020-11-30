package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.grammar.CollectingErrorListener;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.Assert;
import org.junit.Test;

public class ParserErrorReportingTest
{
    @Test
    public void simpleErrorReporting()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

        parserHelper.toExpression("5 + ");

        Assert.assertTrue(errorListener.hasErrors());
        Assert.assertEquals(1, errorListener.getErrors().size());

        CollectingErrorListener.Error error = errorListener.getErrors().get(0);

        Assert.assertEquals(1, error.line());
        Assert.assertEquals(4, error.charPositionInLine());
    }

    @Test
    public void simpleErrorReporting2()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

        parserHelper.toExpression(" and x");

        Assert.assertTrue(errorListener.hasErrors());
        Assert.assertEquals(1, errorListener.getErrors().size());

        CollectingErrorListener.Error error = errorListener.getErrors().get(0);

        Assert.assertEquals(1, error.line());
        Assert.assertEquals(1, error.charPositionInLine());
    }


    @Test
    public void simpleErrorReporting3()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

        parserHelper.toScript("function boo boo()\n" +
                "{\n" +
                "  if a > 7\n" +
                "    x = 'greater'\n" +
                "  else\n" +
                "    x = 'lesser'\n" +
                "  endif\n" +
                "  (x == 'greater') ? 'yes' }\n");

        Assert.assertTrue(errorListener.hasErrors());

        Assert.assertEquals(3, errorListener.getErrors().size());

        MutableList<IntIntPair> errorPositions = Lists.mutable.of(
                PrimitiveTuples.pair(1, 13), PrimitiveTuples.pair(4, 4), PrimitiveTuples.pair(8, 27));

        errorListener.getErrors().forEachInBoth(errorPositions, (e, a) -> {
            Assert.assertEquals(e.briefErrorMessage(), a.getOne(), e.line());
            Assert.assertEquals(e.briefErrorMessage(), a.getTwo(), e.charPositionInLine());
        });

        ListIterable<Integer> tokenLengths = Lists.immutable.of(3, 1, 1);
        errorListener.getErrors().forEachInBoth(tokenLengths, (e, a) -> {
            Assert.assertEquals((int) a, e.tokenStopIndex() - e.tokenStartIndex() + 1);
        });
    }
}
