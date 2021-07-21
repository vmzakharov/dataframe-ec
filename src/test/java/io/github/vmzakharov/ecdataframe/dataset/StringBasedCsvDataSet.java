package io.github.vmzakharov.ecdataframe.dataset;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

public class StringBasedCsvDataSet
extends CsvDataSet
{
    private final String data;
    private final StringWriter writer = new StringWriter();

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
    {
        return new StringReader(this.data);
    }

    @Override
    protected Writer createWriter()
    {
        this.writer.getBuffer().setLength(0); // to mimic the behavior of the superclass
        return this.writer;
    }

    public String getWrittenData()
    {
        return this.writer.toString();
    }
}
