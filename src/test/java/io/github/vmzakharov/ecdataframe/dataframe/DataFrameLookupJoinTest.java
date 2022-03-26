package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataFrameLookupJoinTest
{
    static private DataFrame countries;

    @BeforeClass
    public static void setUpCountries()
    {
        countries = new DataFrame("countries")
                .addStringColumn("ISO Code").addLongColumn("Code").addStringColumn("Country Name").addLongColumn("Number")
                .addRow("BM", 1,     "Bermuda",  60)
                .addRow("NO", 2,      "Norway", 578)
                .addRow("NZ", 1, "New Zealand", 554)
                .addRow("UZ", 2,  "Uzbekistan", 860)
                ;
    }

    @Test
    public void simpleLookup()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO")
                .addRow("1", "Alice", "BM")
                .addRow("3",  "Carl", "XX")
                .addRow("4", "Doris", "NZ")
                ;

        clients.lookup(
            DfJoin.to(countries)
                .match("Country ISO", "ISO Code")
                .select("Country Name")
        );

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO").addStringColumn("Country Name")
                        .addRow("1", "Alice", "BM", "Bermuda")
                        .addRow("3",  "Carl", "XX",  null)
                        .addRow("4", "Doris", "NZ", "New Zealand")
                , clients
        );
    }

    @Test
    public void lookupWithFluentApi()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO")
                .addRow("1", "Alice", "BM")
                .addRow("3",  "Carl", "XX")
                .addRow("4", "Doris", "NZ")
                ;

        clients.lookupIn(countries)
                .match("Country ISO", "ISO Code")
                .select("Country Name")
                .resolveLookup();

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO").addStringColumn("Country Name")
                        .addRow("1", "Alice", "BM", "Bermuda")
                        .addRow("3",  "Carl", "XX",  null)
                        .addRow("4", "Doris", "NZ", "New Zealand")
                , clients
        );
    }

    @Test
    public void lookupWithDefaults()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO")
                .addRow("1", "Alice", "BM")
                .addRow("2",   "Bob", "BM")
                .addRow("3",  "Carl", "XX")
                .addRow("4", "Doris", "NZ")
                .addRow("5",  "Evan", "UZ")
                ;

        clients.lookup(
            DfJoin.to(countries)
                .match("Country ISO", "ISO Code")
                .select("Country Name", "Country")
                .ifAbsent("Not found")
        );

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO").addStringColumn("Country")
                        .addRow("1", "Alice", "BM", "Bermuda")
                        .addRow("2",   "Bob", "BM", "Bermuda")
                        .addRow("3",  "Carl", "XX", "Not found")
                        .addRow("4", "Doris", "NZ", "New Zealand")
                        .addRow("5",  "Evan", "UZ", "Uzbekistan"),
                clients
        );
    }

    @Test
    public void multiColumnLookupMultiColumnSelect()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("User").addStringColumn("Country ISO").addLongColumn("Code")
                .addRow("1", "Alice", "BM", 1)
                .addRow("4", "Doris", "NZ", 1)
                .addRow("5",  "Evan", "UZ", 2)
                ;

        DataFrame enriched = clients.lookup(
                DfJoin.to(countries)
                      .match(Lists.immutable.of("Country ISO", "Code"), Lists.immutable.of("ISO Code", "Code"))
                      .select(Lists.immutable.of("Country Name", "Number"))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("User").addStringColumn("Country ISO").addLongColumn("Code")
                        .addStringColumn("Country Name").addLongColumn("Number")
                        .addRow("1", "Alice", "BM", 1,     "Bermuda",  60)
                        .addRow("4", "Doris", "NZ", 1, "New Zealand", 554)
                        .addRow("5",  "Evan", "UZ", 2,  "Uzbekistan", 860),
                enriched
        );
    }

    @Test
    public void multiColumnLookupMultiColumnSelectWithAliases()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO").addLongColumn("Code")
                .addRow("1", "Alice", "BM", 1)
                .addRow("3",  "Carl", "XX", 1)
                .addRow("2",   "Bob", "BM", 1)
                .addRow("4", "Doris", "NZ", 1)
                .addRow("5",  "Evan", "UZ", 2)
                ;

        clients
            .lookup(
                DfJoin.to(countries)
                      .match(Lists.immutable.of("Country ISO", "Code"), Lists.immutable.of("ISO Code", "Code"))
                      .select(Lists.immutable.of("Country Name", "Number"), Lists.immutable.of("Country", "Country Code"))
                      .ifAbsent(Lists.immutable.of("Not found", -1))
            )
            .dropColumn("Code");

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO")
                        .addStringColumn("Country").addLongColumn("Country Code")
                        .addRow("1", "Alice", "BM",     "Bermuda",  60)
                        .addRow("3",  "Carl", "XX",   "Not found",  -1)
                        .addRow("2",   "Bob", "BM",     "Bermuda",  60)
                        .addRow("4", "Doris", "NZ", "New Zealand", 554)
                        .addRow("5",  "Evan", "UZ",  "Uzbekistan", 860),
                clients
        );
    }

    @Test
    public void multiColumnLookupMultiColumnSelectWithAliasesFluentAPI()
    {
        DataFrame clients = new DataFrame("clients")
                .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO").addLongColumn("Code")
                .addRow("1", "Alice", "BM", 1)
                .addRow("3",  "Carl", "XX", 1)
                .addRow("2",   "Bob", "BM", 1)
                .addRow("4", "Doris", "NZ", 1)
                .addRow("5",  "Evan", "UZ", 2)
                ;

        clients
            .lookupIn(countries)
            .match(Lists.immutable.of("Country ISO", "Code"), Lists.immutable.of("ISO Code", "Code"))
            .select(Lists.immutable.of("Country Name", "Number"), Lists.immutable.of("Country", "Country Code"))
            .ifAbsent(Lists.immutable.of("Not found", -1))
            .resolveLookup()
            .dropColumn("Code");

        DataFrameUtil.assertEquals(
                new DataFrame("clients")
                        .addStringColumn("Id").addStringColumn("Name").addStringColumn("Country ISO")
                        .addStringColumn("Country").addLongColumn("Country Code")
                        .addRow("1", "Alice", "BM",     "Bermuda",  60)
                        .addRow("3",  "Carl", "XX",   "Not found",  -1)
                        .addRow("2",   "Bob", "BM",     "Bermuda",  60)
                        .addRow("4", "Doris", "NZ", "New Zealand", 554)
                        .addRow("5",  "Evan", "UZ",  "Uzbekistan", 860),
                clients
        );
    }
}
