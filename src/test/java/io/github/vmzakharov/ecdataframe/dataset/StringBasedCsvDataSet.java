package io.github.vmzakharov.ecdataframe.dataset;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class StringBasedCsvDataSet
extends CsvDataSet
{
    private final String data;

    public StringBasedCsvDataSet(String dataFileName, String newName, String newData)
    {
        super(dataFileName, newName);
        this.data = newData;
    }

    public StringBasedCsvDataSet(String dataFileName, String newName, CsvSchema newSchema, String newData)
    {
        super(dataFileName, newName, newSchema);
        this.data = newData;
    }

    @Override
    protected Reader createReader()
    throws IOException
    {
        return new StringReader(this.data);
    }
}
