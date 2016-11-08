package com.java.avro.util.test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.java.avro.util.BigQuerySchemaConverter;

public class BigQuerySchemaConverterTest {

	private static final Logger LOG = LoggerFactory.getLogger(BigQuerySchemaConverterTest.class);

	private final BigQuerySchemaConverter converter = BigQuerySchemaConverter.getInstance();

	@Test
	public void testNullInput() {
		try {
			Schema schema = converter.toAvroSchema(null);
			Assert.assertNotNull(schema);
			Assert.assertEquals(schema.getName(), "Root");
		} catch (Exception e) {
		}
	}

	@Test
	public void testBigTable() throws Exception {
		final Table table = getTableWithSchema();
		Schema schema = converter.toAvroSchema(table);
		Assert.assertNotNull(schema);
		Assert.assertNotNull(schema.getName());
		Assert.assertNotNull(schema.getField("name"));
		Assert.assertNotNull(schema.getField("address"));
		Assert.assertEquals(schema.getField("address").schema().getType(), Type.ARRAY);
		Assert.assertNotNull(schema.getField("salary"));
		Assert.assertNotNull(schema.getField("email"));
		Assert.assertEquals(schema.getField("email").schema().getType(), Type.UNION);
		Assert.assertEquals(schema.getField("email").schema(),
				Schema.createUnion(Arrays.asList(Schema.create(Type.STRING), Schema.create(Type.NULL))));
		Assert.assertEquals(schema.getField("address").schema().getType(), Type.ARRAY);
		Assert.assertNotNull(schema.getField("address").schema().getElementType().getField("line1"));
		Assert.assertNotNull(schema.getField("address").schema().getElementType().getField("line2"));
		Assert.assertNotNull(schema.getField("address").schema().getElementType().getField("zipcode"));
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getNamespace(), "root");
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getFields().size(), 4);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getType(), Type.RECORD);
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value1"));
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value2"));
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value3"));
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value4"));
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value1").schema().getType(), Type.UNION);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value2").schema().getType(), Type.STRING);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value3").schema().getType(), Type.BOOLEAN);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value4").schema().getType(), Type.FLOAT);
		Assert.assertEquals(schema.getField("internalRecord").schema().getElementType().getField("dimension").schema().getType(), Type.ARRAY);
		Assert.assertEquals(
				schema.getField("internalRecord").schema().getElementType().getField("dimension").schema().getElementType().getNamespace(),
				"root.internalrecord");
		Assert.assertNotNull(
				schema.getField("internalRecord").schema().getElementType().getField("dimension").schema().getElementType().getField("value1"));
		Assert.assertNotNull(
				schema.getField("internalRecord").schema().getElementType().getField("dimension").schema().getElementType().getField("value2"));
		Assert.assertNotNull(schema.getField("metrics"));
		Assert.assertNotNull(schema.getField("metrics").schema().getTypes().get(0).getField("metricValue"));
		Assert.assertEquals(schema.getField("metrics").schema().getTypes().get(0).getField("metricValue").schema().getType(), Type.UNION);
		Assert.assertEquals(schema.getField("metrics").schema().getTypes().get(0).getField("metricValue").schema().getTypes().get(0),
				Schema.create(Type.DOUBLE));

		LOG.info(schema.toString());
	}

	@Test
	public void testNullType() throws IOException, GeneralSecurityException {
		final TableSchema tableSchema = new TableSchema();
		final List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("name").setType("NULL").setMode("REQUIRED"));
		tableSchema.setFields(fields);

		final Schema schema = converter.toAvroSchema(new Table().setSchema(tableSchema));

		Assert.assertNotNull(schema);
		Assert.assertNotNull(schema.getName());
		Assert.assertNotNull(schema.getField("name"));
		Assert.assertNotNull(schema.getField("name").schema().getType());
		Assert.assertTrue(schema.getField("name").schema().getType().equals(Type.NULL));
	}

	@Test
	public void testNotValidFieldType() {
		final TableSchema tableSchema = new TableSchema();
		final List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("name").setType("NO-TYPE").setMode("REQUIRED"));
		tableSchema.setFields(fields);

		try {
			converter.toAvroSchema(new Table().setSchema(tableSchema));
		} catch (Exception e) {
			Assert.assertTrue(e instanceof AvroRuntimeException);
			Assert.assertEquals(e.getMessage(), "Not valid avro type for the big query type: 'NO-TYPE'");
		}
	}

	@Test
	public void testTimestampFieldType() {
		final TableSchema tableSchema = new TableSchema();
		final List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("date").setType("DATE").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("datetime").setType("DATETIME").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("time").setType("TIME").setMode("REQUIRED"));
		tableSchema.setFields(fields);

		try {
			final Schema schema = converter.toAvroSchema(new Table().setSchema(tableSchema));
			Assert.assertEquals(schema.getField("date").schema().getType(), Type.STRING);
			Assert.assertEquals(schema.getField("timestamp").schema().getType(), Type.STRING);
			Assert.assertEquals(schema.getField("time").schema().getType(), Type.STRING);
			Assert.assertEquals(schema.getField("datetime").schema().getType(), Type.STRING);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	@Test
	public void testBytesFieldType() {
		final TableSchema tableSchema = new TableSchema();
		final List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("bytes").setType("BYTES").setMode("REQUIRED"));
		tableSchema.setFields(fields);

		try {
			final Schema schema = converter.toAvroSchema(new Table().setSchema(tableSchema));
			Assert.assertEquals(schema.getField("bytes").schema().getType(), Type.BYTES);
		} catch (Exception e) {
			Assert.fail();
		}
	}

	private static Table getTableWithSchema() {
		final TableSchema tableSchema = new TableSchema();
		final List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("age").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("birthday").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("salary").setType("LONG").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("email").setType("STRING").setMode("NULLABLE"));

		final List<TableFieldSchema> addressList = new ArrayList<>();
		addressList.add(new TableFieldSchema().setName("line1").setType("STRING").setMode("REQUIRED"));
		addressList.add(new TableFieldSchema().setName("line2").setType("STRING").setMode("NULLABLE"));
		addressList.add(new TableFieldSchema().setName("zipcode").setType("STRING").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("address").setType("RECORD").setMode("REPEATED").setFields(addressList));

		final List<TableFieldSchema> array = new ArrayList<>();
		array.add(new TableFieldSchema().setName("value1").setType("INTEGER").setMode("NULLABLE"));
		array.add(new TableFieldSchema().setName("value2").setType("STRING").setMode("REQUIRED"));
		array.add(new TableFieldSchema().setName("value3").setType("BOOLEAN").setMode("REQUIRED"));
		array.add(new TableFieldSchema().setName("value4").setType("FLOAT").setMode("REQUIRED"));
		final TableFieldSchema internalRecord = new TableFieldSchema().setName("dimension").setType("RECORD").setMode("REPEATED").setFields(array);
		fields.add(internalRecord);

		fields.add(new TableFieldSchema().setName("metrics").setType("RECORD").setMode("NULLABLE").setFields(
				Collections.singletonList(
						new TableFieldSchema().setName("metricValue").setType("DOUBLE").setMode("NULLABLE"))));

		fields.add(new TableFieldSchema().setName("emptyRecord").setType("RECORD").setMode("NULLABLE").setFields(new ArrayList<>()));
		fields.add(new TableFieldSchema().setName("nullFiledsRecord").setType("RECORD").setMode("NULLABLE").setFields(null));

		fields.add(new TableFieldSchema().setName("internalRecord").setType("RECORD").setMode("REPEATED")
				.setFields(Collections.singletonList(internalRecord)));

		tableSchema.setFields(fields);

		final Table table = new Table();
		table.setSchema(tableSchema);

		return table;
	}

}
