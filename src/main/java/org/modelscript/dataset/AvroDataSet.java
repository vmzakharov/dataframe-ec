package org.modelscript.dataset;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.eclipse.collections.impl.utility.ListIterate;
import org.modelscript.expr.value.ValueType;

import java.io.File;
import java.io.IOException;

public class AvroDataSet
extends DataSetAbstract
{
    private final Schema schema;
    private final String dataFileName;

    private GenericRecord currentRecord;
    private DataFileReader<GenericRecord> dataFileReader;

    public AvroDataSet(String schemaDefinitionFileName, String newName, String newDataFileName)
    {
        super(newName);
        this.schema = this.loadSchema(schemaDefinitionFileName);
        this.dataFileName = newDataFileName;
    }

    private Schema loadSchema(String schemaDefinitionFileName)
    {
        try
        {
            return new Schema.Parser().parse(new File(schemaDefinitionFileName));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load a schema definition from " + schemaDefinitionFileName + "'", e);
        }
    }

    public GenericRecord createRecord()
    {
        return new GenericData.Record(this.schema);
    }

    public GenericRecord createRecordForField(String fieldName)
    {
        return new GenericData.Record(this.findFieldSchema(fieldName));
    }

    private Schema findFieldSchema(String fieldName)
    {
        Schema fieldSchema = schema.getField(fieldName).schema();

        if (fieldSchema.isUnion())
        {
            fieldSchema = ListIterate.detect(fieldSchema.getTypes(), schema -> schema.getType() != Schema.Type.NULL);
        }

        return fieldSchema;
    }

    public void write(GenericRecord... records)
    {

        File file = new File(this.dataFileName);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);)
        {
            dataFileWriter.create(this.schema, file);
            for (int i = 0; i < records.length; i++)
            {
                dataFileWriter.append(records[i]);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to persist data into '" + this.dataFileName + "'", e);
        }
    }

    @Override
    public void openFileForReading()
    {
        File file = new File(this.dataFileName);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(this.schema);
        try
        {
            this.dataFileReader = new DataFileReader<>(file, datumReader);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to create a reader for file '" + this.dataFileName + "'", e);
        }
    }

    public Object getValue(String propertyChainString)
    {
        return this.currentRecord.get(propertyChainString);
    }

    @Override
    public Object next()
    {
        this.currentRecord = this.dataFileReader.next();
        return this.currentRecord;
    }

    @Override
    public boolean hasNext()
    {
        return this.dataFileReader.hasNext();
    }

    @Override
    public void close()
    {
        try
        {
            this.dataFileReader.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to close data file reader for '" + dataFileName + "'");
        }
    }

    public ValueType getFieldType(String fieldName)
    {
        Schema fieldSchema = this.findFieldSchema(fieldName);
        switch (fieldSchema.getType())
        {
            case INT:
                return ValueType.LONG;
            case DOUBLE:
            case FLOAT:
            case FIXED:
                return ValueType.DOUBLE;
            case STRING:
                return ValueType.STRING;
            case BOOLEAN:
                return ValueType.BOOLEAN;
            default:
                return ValueType.VOID;
        }
    }
}
