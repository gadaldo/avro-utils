package com.java.avro.util.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.java.avro.util.BigQueryToAvroSchema;

public class BigQueryToAvroUtilTest {

	private static final Logger LOG = LoggerFactory.getLogger(BigQueryToAvroUtilTest.class);

	private final BigQueryToAvroSchema converter = BigQueryToAvroSchema.getInstance();

	@Test
	public void testNullInput() {
		try {
			Schema schema = converter.buildSchema(null);
			Assert.assertNotNull(schema);
			Assert.assertEquals(schema.getName(), "Root");
		} catch (Exception e) {
		}
	}

	@Test
	public void testBigTable() throws Exception {
		final Table table = getTableWithSchema();
		Schema schema = converter.buildSchema(table);
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
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getFields().size(), 2);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getType(), Type.RECORD);
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value1"));
		Assert.assertNotNull(schema.getField("dimension").schema().getElementType().getField("value2"));
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value1").schema().getType(), Type.UNION);
		Assert.assertEquals(schema.getField("dimension").schema().getElementType().getField("value2").schema().getType(), Type.STRING);
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

		LOG.info(schema.toString());
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
		final TableFieldSchema internalRecord = new TableFieldSchema().setName("dimension").setType("RECORD").setMode("REPEATED").setFields(array);
		fields.add(internalRecord);

		fields.add(new TableFieldSchema().setName("metrics").setType("RECORD").setMode("NULLABLE").setFields(
				Collections.singletonList(
						new TableFieldSchema().setName("metricValue").setType("LONG").setMode("NULLABLE"))));

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
