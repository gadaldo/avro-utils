Avro Util
---

This simple maven project is a utility project to build **Avro** schema from an **Google Bigquery** table
and to write any JSON data string into a _GenericRecord_ (AVRO format) so that it is also useful to 
translate _TableRow_ to _GenericRecord_ when you have to read from Bigquery and write in AVRO format.

In Google Cloud Platform there is no such utility so this would help in case you need.

version: _DRAFT_

Sample usage
---

In a transformation process: 

```java

	public void processElement(ProcessContext c) throws IOException {
		final TableRow row = c.element();

		final GenericRecord record = new JsonGenericRecordReader().read(row, schema);

		c.output(record);
	}
```

Table schema conversion: 

```java
	
	private final BigQueryToAvroSchema converter = BigQuerySchemaConverter.getInstance();
	
	private Schema getAvroSchema(TableSchema tableSchema) {
		final Table table = getTableWithSchema();
		final Schema schema = converter.toAvroSchema(table);
		return schema;
	}
```

Seen this has been tested on some internal Google Bigquery tables, 
any review, bug or feedback is welcome so please contribute to it writing here.