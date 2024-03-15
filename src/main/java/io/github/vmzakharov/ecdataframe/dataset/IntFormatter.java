package io.github.vmzakharov.ecdataframe.dataset;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class IntFormatter
{
    private DecimalFormat decimalFormat;
    private IntFunction<String> intParser;
    private Pattern nonNumericPattern;

    public IntFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.intParser = Integer::parseInt;
        }
        else if (pattern.length() == 1)
        {
            this.nonNumericPattern = Pattern.compile("[^0-9" + pattern + "]");
            this.intParser = s -> Integer.parseInt(this.stripNonNumericCharacters(s));
        }
        else
        {
            this.decimalFormat = new DecimalFormat(pattern);
            this.intParser = s -> {
                try
                {
                    return this.decimalFormat.parse(s.trim()).intValue();
                }
                catch (ParseException e)
                {
                    throw exceptionByKey("CSV_PARSE_ERR")
                            .with("type", "integer")
                            .with("inputString", s)
                            .get(e);
                }
            };
        }
    }

    public int parseAsInt(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return 0;
        }

        return this.intParser.applyAsInt(aString);
    }

    public String format(long longValue)
    {
        return this.decimalFormat == null ? Long.toString(longValue) : this.decimalFormat.format(longValue);
    }

    private String stripNonNumericCharacters(String s)
    {
        return this.nonNumericPattern.matcher(s).replaceAll("");
    }
}
