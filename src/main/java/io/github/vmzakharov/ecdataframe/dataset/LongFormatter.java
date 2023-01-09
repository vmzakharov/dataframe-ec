package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.util.ErrorReporter;
import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

public class LongFormatter
{
    private DecimalFormat decimalFormat;
    private LongFunction<String> longParser;
    private Pattern nonNumericPattern;

    public LongFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.longParser = Long::parseLong;
        }
        else if (pattern.length() == 1)
        {
            this.nonNumericPattern = Pattern.compile("[^0-9" + pattern + "]");
            this.longParser = s -> Long.parseLong(this.stripNonNumericCharacters(s));
        }
        else
        {
            this.decimalFormat = new DecimalFormat(pattern);
            this.longParser = s -> {
                try
                {
                    return this.decimalFormat.parse(s.trim()).longValue();
                }
                catch (ParseException e)
                {
                    throw ErrorReporter.exception("Failed to parse input string to integer: '${inputString}'")
                            .with("inputString", s).get(e);
                }
            };
        }
    }

    public long parseAsLong(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return 0L;
        }

        return this.longParser.applyAsLong(aString);
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
