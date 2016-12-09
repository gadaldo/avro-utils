package com.java.avro.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Avro schema convert utility.
 * 
 * @author giuseppe.adaldo
 *
 */
public class AvroSchemaConverter {

	/**
	 * onvert Avro schema to bigquery schema.
	 * 
	 * @param schema source avro schema to convert
	 * @return
	 */
	public static TableSchema toTableSchema(Schema schema) {
		return toTableSchema(schema, Collections.emptyList());
	}

	/**
	 * Convert Avro schema to bigquery schema skipping the fields specified into the given parameter.
	 * 
	 * @param schema source avro schema to convert
	 * @param fieldsToSkip fields to skip converting
	 * @return {@link TableSchema} object
	 */
	public static TableSchema toTableSchema(Schema schema, String... fieldsToSkip) {
		return toTableSchema(schema, Arrays.asList(fieldsToSkip));
	}

	/**
	 * Convert Avro schema to bigquery schema skipping the fields specified into the given parameter list.
	 * 
	 * @param schema source avro schema to convert
	 * @param fieldsToSkip list of fields to skip converting
	 * @return {@link TableSchema} object
	 */
	public static TableSchema toTableSchema(Schema schema, List<String> fieldsToSkip) {
		return Optional.ofNullable(fieldsToSkip)
				.map(toSkip -> new TableSchema().setFields(
						getTableFieldSchemas(
								schema.getFields(),
								field -> !fieldsToSkip.contains(field.name()))))
				.orElseGet(() -> toTableSchema(schema));
	}

	private static List<TableFieldSchema> getTableFieldSchemas(List<Field> fields, Predicate<Field> isSkippableFn) {
		final List<TableFieldSchema> tempList = fields.stream()
				.filter(isSkippableFn)
				.map(field -> getTableFieldSchema(field, isSkippableFn))
				.collect(Collectors.toList());
		return tempList.isEmpty() ? null : tempList;
	}

	private static TableFieldSchema getTableFieldSchema(Field field, Predicate<Field> isSkippableFn) {
		final TableFieldSchema tableFieldSchema = new TableFieldSchema();
		tableFieldSchema.setMode("REQUIRED");
		tableFieldSchema.setName(field.name());

		final Type type = field.schema().getType();
		tableFieldSchema.setType(getTableSchemaType(type));

		switch (type) {
		case ARRAY:
			tableFieldSchema.setMode("REPEATED");
			convertArray(tableFieldSchema, field, isSkippableFn);
			return tableFieldSchema;
		case RECORD:
			tableFieldSchema.setFields(getTableFieldSchemas(field.schema().getFields(), isSkippableFn));
			return tableFieldSchema;
		case UNION:
			convertUnion(tableFieldSchema, field, isSkippableFn);
			return tableFieldSchema;
		default:
			return tableFieldSchema;
		}
	}

	private static final void convertUnion(TableFieldSchema tableFieldSchema, Schema.Field field, Predicate<Field> isSkippableFn) {
		for (Schema unionSchema : field.schema().getTypes()) {
			if (unionSchema.getType().equals(Type.NULL)) {
				tableFieldSchema.setMode("NULLABLE");
			} else if (unionSchema.getType() == Type.RECORD) {
				tableFieldSchema.setType("RECORD");
				tableFieldSchema.setFields(getTableFieldSchemas(unionSchema.getFields(), isSkippableFn));
			} else if (unionSchema.getType() == Type.ARRAY) {
				tableFieldSchema.setType("RECORD");
				tableFieldSchema.setFields(getTableFieldSchemas(unionSchema.getElementType().getFields(), isSkippableFn));
			} else {
				tableFieldSchema.setType(getTableSchemaType(unionSchema.getType()));
			}
		}
	}

	private static void convertArray(TableFieldSchema tableFieldSchema, Schema.Field field, Predicate<Field> isSkippableFn) {
		if (field.schema().getElementType().getType() == Type.RECORD)
			tableFieldSchema.setFields(getTableFieldSchemas(field.schema().getElementType().getFields(), isSkippableFn));
	}

	private static String getTableSchemaType(Type type) {
		switch (type) {
		case ARRAY:
		case MAP:
		case RECORD:
			return "RECORD";
		case BOOLEAN:
			return "BOOLEAN";
		case BYTES:
			return "BYTES";
		case DOUBLE:
		case FLOAT:
			return "FLOAT";
		case INT:
		case LONG:
			return "INTEGER";
		case UNION:
			return "NULL";
		default:
			return "STRING";
		}
	}

}