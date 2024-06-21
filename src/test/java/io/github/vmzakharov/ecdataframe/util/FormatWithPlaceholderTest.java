package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.ResourceBundle;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.UNKNOWN_KEY_MESSAGE;
import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.message;
import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.messageFromKey;
import static org.junit.jupiter.api.Assertions.*;

public class FormatWithPlaceholderTest
{
    @Test
    public void noPlaceholders()
    {
        assertEquals("Hello. How are you?", message("Hello. How are you?").toString());
    }

    @Test
    public void onePlaceholder()
    {
        assertEquals("Hello, Alice. How are you?",
                message("Hello, ${name}. How are you?").with("name", "Alice").toString());
    }

    @Test
    public void manyPlaceholders()
    {
        assertEquals("Hello, Alice. How are you today?",
                message("Hello, ${name}. How are you ${time}?")
                        .with("name", "Alice").with("time", "today").toString());
    }

    @Test
    public void repeatingPlaceholders()
    {
        assertEquals("Hello, Alice. How are you today, Alice?",
                message("Hello, ${name}. How are you ${time}, ${name}?").with("name", "Alice").with("time", "today").toString());
    }

    @Test
    public void missingPlaceholder()
    {
        assertEquals("Hello, (name is unknown). How are you?",
                message("Hello, ${name}. How are you?").toString());

        assertEquals("Hello, (name is unknown). How are you today, (name is unknown)?",
                message("Hello, ${name}. How are you ${time}, ${name}?").with("time", "today").toStringSupplier().get());
    }

    @Test
    public void withResourceBundle()
    {
        FormatWithPlaceholders.addMessagesFromResourceBundle(ResourceBundle.getBundle("Messages"));

        assertEquals("Hello, Alice! How are you?", messageFromKey("GREETING").with("name", "Alice").toString());

        assertEquals("GREETINGS PROFESSOR FALKEN.", messageFromKey("SALUTATION").with("lastName", "FALKEN").toString());
    }

    @Test
    public void withProperties()
    {
        Properties props = new Properties();
        props.setProperty("GREETING", "Hello, ${name}! How are you?");
        props.setProperty("SALUTATION", "GREETINGS PROFESSOR ${lastName}.");

        FormatWithPlaceholders.addMessagesFromProperties(props);

        assertEquals("Hello, Alice! How are you?", messageFromKey("GREETING").with("name", "Alice").toString());

        assertEquals("GREETINGS PROFESSOR FALKEN.", messageFromKey("SALUTATION").with("lastName", "FALKEN").toString());
    }

    @Test
    public void withMap()
    {
        MutableMap<String, String> messagesByKey = Maps.mutable.with(
            "GREETING", "Hello, ${name}! How are you?",
            "SALUTATION", "GREETINGS PROFESSOR ${lastName}."
        );

        FormatWithPlaceholders.addMessagesFromMap(messagesByKey);

        assertEquals("Hello, ${Alice}! How are you?", messageFromKey("GREETING").with("name", "${Alice}").toString());

        assertEquals("GREETINGS PROFESSOR JENKINS.", messageFromKey("SALUTATION").with("lastName", "JENKINS").toString());
    }

    @Test
    public void unknownKey()
    {
        String key = "Oompa Loompa";
        assertEquals(String.format(UNKNOWN_KEY_MESSAGE, key), messageFromKey(key).toString());
    }
}
