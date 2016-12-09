package com.java.avro.util;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

// This class was developed to resolve the bugs encountered with using Google's AvroUtils class, which this class extends.
public class AvroUtils extends com.google.cloud.dataflow.sdk.util.AvroUtils {

	private static final ImmutableMap<String, List<Schema.Type>> BQ_TYPE_TO_AVRO_TYPE_MAP = ImmutableMap.<String, List<Schema.Type>>builder()
			.put("BOOLEAN", Arrays.asList(Schema.Type.BOOLEAN))
			.put("BYTES", Arrays.asList(Schema.Type.BYTES, Schema.Type.FIXED))
			.put("DATE", Arrays.asList(Schema.Type.STRING))
			.put("DATETIME", Arrays.asList(Schema.Type.STRING))
			.put("FLOAT", Arrays.asList(Schema.Type.DOUBLE, Schema.Type.FLOAT))
			.put("INTEGER", Arrays.asList(Schema.Type.INT, Schema.Type.LONG))
			.put("RECORD", Arrays.asList(Schema.Type.RECORD))
			.put("STRING", Arrays.asList(Schema.Type.ENUM, Schema.Type.STRING))
			.put("TIME", Arrays.asList(Schema.Type.STRING))
			.put("TIMESTAMP", Arrays.asList(Schema.Type.LONG))
			.build();

	/**
	 * Reflection has been used to keep compatibility with google AvroUtils.
	 * 
	 * @param timestamp
	 * @return
	 */
	// Package private for BigQueryTableRowIterator to use.
	private static String formatTimestamp(String timestamp) {
		try {
			final Method m = com.google.cloud.dataflow.sdk.util.AvroUtils.class.getDeclaredMethod("formatTimestamp", String.class);
			m.setAccessible(true);
			return (String) m.invoke(null, timestamp);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
		return convertGenericRecordToTableRow(record, schema.getFields());
	}

	private static TableRow convertGenericRecordToTableRow(GenericRecord record, List<TableFieldSchema> fields) {
		TableRow row = new TableRow();
		for (TableFieldSchema subSchema : fields) {
			// Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the name field
			// is required, so it may not be null.
			Schema.Field field = record.getSchema().getField(subSchema.getName());
			Object convertedValue = getTypedCellValue(field.schema(), subSchema, record.get(field.name()));
			if (convertedValue != null) {
				// To match the JSON files exported by BigQuery, do not include null values in the output.
				row.set(field.name(), convertedValue);
			}
		}
		return row;
	}

	@Nullable
	private static Object getTypedCellValue(Schema schema, TableFieldSchema fieldSchema, Object v) {
		// Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the mode field
		// is optional (and so it may be null), but defaults to "NULLABLE".
		String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");
		switch (mode) {
		case "REQUIRED":
			return convertRequiredField(schema.getType(), fieldSchema, v);
		case "REPEATED":
			return convertRepeatedField(schema, fieldSchema, v);
		case "NULLABLE":
			return convertNullableField(schema, fieldSchema, v);
		default:
			throw new UnsupportedOperationException(
					"Parsing a field with BigQuery field schema mode " + fieldSchema.getMode());
		}
	}

	private static List<Object> convertRepeatedField(Schema schema, TableFieldSchema fieldSchema, Object v) {
		Schema.Type arrayType = schema.getType();
		verify(
				arrayType == Schema.Type.ARRAY,
				"BigQuery REPEATED field %s should be Avro ARRAY, not %s",
				fieldSchema.getName(),
				arrayType);
		// REPEATED fields are represented as Avro arrays.
		if (v == null) {
			// Handle the case of an empty repeated field.
			return ImmutableList.of();
		}
		@SuppressWarnings("unchecked")
		List<Object> elements = (List<Object>) v;
		ImmutableList.Builder<Object> values = ImmutableList.builder();
		Schema.Type elementType = schema.getElementType().getType();
		for (Object element : elements) {
			values.add(convertRequiredField(elementType, fieldSchema, element));
		}
		return values.build();
	}

	private static Object convertRequiredField(Schema.Type avroType, TableFieldSchema fieldSchema, Object v) {
		// REQUIRED fields are represented as the corresponding Avro types. For example, a BigQuery
		// INTEGER type maps to an Avro LONG type.
		checkNotNull(v, "REQUIRED field %s should not be null", fieldSchema.getName());

		// Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the type field
		// is required, so it may not be null.
		String bqType = fieldSchema.getType();
		List<Schema.Type> expectedAvroTypes = BQ_TYPE_TO_AVRO_TYPE_MAP.get(bqType);
		verifyNotNull(expectedAvroTypes, "Unsupported BigQuery type: %s", bqType);
		verify(
				expectedAvroTypes.contains(avroType),
				"Expected Avro schema type %s, not %s, for BigQuery %s field %s",
				expectedAvroTypes,
				avroType,
				bqType,
				fieldSchema.getName());
		switch (fieldSchema.getType()) {
		case "BOOLEAN":
			verify(v instanceof Boolean, "Expected Boolean, got %s", v.getClass());
			return v;
		case "BYTES":
			verify(v instanceof ByteBuffer, "Expected ByteBuffer, got %s", v.getClass());
			ByteBuffer byteBuffer = (ByteBuffer) v;
			byte[] bytes = new byte[byteBuffer.limit()];
			byteBuffer.get(bytes);
			return BaseEncoding.base64().encode(bytes);
		case "DATE":
		case "DATETIME":
		case "STRING":
		case "TIME":
			// Avro will use a CharSequence to represent String objects, but it may not always use
			// java.lang.String; for example, it may prefer org.apache.avro.util.Utf8.
			verify(v instanceof CharSequence || v instanceof GenericData.EnumSymbol,
					"Expected CharSequence (String) or GenericData.EnumSymbol, got %s", v.getClass());
			return v.toString();
		case "FLOAT":
			verify(v instanceof Double || v instanceof Float, "Expected Double or Float, got %s", v.getClass());
			return v;
		case "INTEGER":
			verify(v instanceof Integer || v instanceof Long, "Expected Integer or Long, got %s", v.getClass());
			return v;
		case "RECORD":
			verify(v instanceof GenericRecord, "Expected GenericRecord, got %s", v.getClass());
			return convertGenericRecordToTableRow((GenericRecord) v, fieldSchema.getFields());
		case "TIMESTAMP":
			// TIMESTAMP data types are represented as Avro LONG types. They are converted back to
			// Strings with variable-precision (up to six digits) to match the JSON files export
			// by BigQuery.
			verify(v instanceof Long, "Expected Long, got %s", v.getClass());
			Double doubleValue = ((Long) v) / 1000000.0;
			return formatTimestamp(doubleValue.toString());
		default:
			throw new UnsupportedOperationException(
					String.format(
							"Unexpected BigQuery field schema type %s for field named %s",
							fieldSchema.getType(),
							fieldSchema.getName()));
		}
	}

	@Nullable
	private static Object convertNullableField(Schema avroSchema, TableFieldSchema fieldSchema, Object v) {
		// NULLABLE fields are represented as an Avro Union of the corresponding type and "null".
		verify(
				avroSchema.getType() == Schema.Type.UNION,
				"Expected Avro schema type UNION, not %s, for BigQuery NULLABLE field %s",
				avroSchema.getType(),
				fieldSchema.getName());
		List<Schema> unionTypes = avroSchema.getTypes();
		verify(
				unionTypes.size() == 2,
				"BigQuery NULLABLE field %s should be an Avro UNION of NULL and another type, not %s",
				fieldSchema.getName(),
				unionTypes);

		if (v == null) {
			return null;
		}

		Schema.Type firstType = unionTypes.get(0).getType();
		if (!firstType.equals(Schema.Type.NULL)) {
			return convertRequiredField(firstType, fieldSchema, v);
		}
		return convertRequiredField(unionTypes.get(1).getType(), fieldSchema, v);
	}

}