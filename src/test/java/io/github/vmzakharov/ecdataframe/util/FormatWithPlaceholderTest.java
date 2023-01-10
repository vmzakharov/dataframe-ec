package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.ResourceBundle;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.format;
import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.formatKey;

public class FormatWithPlaceholderTest
{
    @Test
    public void noPlaceholders()
    {
        Assert.assertEquals("Hello. How are you?", format("Hello. How are you?").toString());
    }

    @Test
    public void onePlaceholder()
    {
        Assert.assertEquals("Hello, Alice. How are you?",
                format("Hello, ${name}. How are you?").with("name", "Alice").toString());
    }

    @Test
    public void manyPlaceholders()
    {
        Assert.assertEquals("Hello, Alice. How are you today?",
                format("Hello, ${name}. How are you ${time}?")
                        .with("name", "Alice").with("time", "today").toString());
    }

    @Test
    public void repeatingPlaceholders()
    {
        Assert.assertEquals("Hello, Alice. How are you today, Alice?",
                format("Hello, ${name}. How are you ${time}, ${name}?").with("name", "Alice").with("time", "today").toString());
    }

    @Test
    public void missingPlaceholder()
    {
        Assert.assertEquals("Hello, (name is unknown). How are you?",
                format("Hello, ${name}. How are you?").toString());

        Assert.assertEquals("Hello, (name is unknown). How are you today, (name is unknown)?",
                format("Hello, ${name}. How are you ${time}, ${name}?").with("time", "today").toStringSupplier().get());
    }

    @Test
    public void withResourceBundle()
    {
        FormatWithPlaceholders.addMessagesFromResourceBundle(ResourceBundle.getBundle("Messages"));

        Assert.assertEquals("Hello, Alice! How are you?", formatKey("GREETING").with("name", "Alice").toString());

        Assert.assertEquals("GREETINGS PROFESSOR FALKEN.", formatKey("SALUTATION").with("lastName", "FALKEN").toString());
    }

    @Test
    public void withProperties()
    {
        Properties props = new Properties();
        props.setProperty("GREETING", "Hello, ${name}! How are you?");
        props.setProperty("SALUTATION", "GREETINGS PROFESSOR ${lastName}.");

        FormatWithPlaceholders.addMessagesFromProperties(props);

        Assert.assertEquals("Hello, Alice! How are you?", formatKey("GREETING").with("name", "Alice").toString());

        Assert.assertEquals("GREETINGS PROFESSOR FALKEN.", formatKey("SALUTATION").with("lastName", "FALKEN").toString());
    }

    @Test
    public void withMap()
    {
        MutableMap<String, String> messagesByKey = Maps.mutable.with(
            "GREETING", "Hello, ${name}! How are you?",
            "SALUTATION", "GREETINGS PROFESSOR ${lastName}."
        );

        FormatWithPlaceholders.addMessagesFromMap(messagesByKey);

        Assert.assertEquals("Hello, ${Alice}! How are you?", formatKey("GREETING").with("name", "${Alice}").toString());

        Assert.assertEquals("GREETINGS PROFESSOR JENKINS.", formatKey("SALUTATION").with("lastName", "JENKINS").toString());
    }
}
