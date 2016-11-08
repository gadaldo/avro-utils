package com.java.avro.util;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.avro.Schema;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.java.avro.util.AvroSchemaObject.AvroField;
import com.java.avro.util.AvroSchemaObject.AvroRecord;

/**
 * Utility to transform a BigQuery table schema to an AVRO schema.
 * 
 * @author giuseppe.adaldo
 *
 */
public enum BigQuerySchemaConverter {

	INSTANCE;

	/**
	 * Returns the {@link Schema} representation of the given Bigquery {@link Table} object.
	 * 
	 * @param table
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public Schema toAvroSchema(Table table) throws IOException, GeneralSecurityException {
		final AvroSchemaObject schemaObject = AvroSchemaObject.build("Root", "");
		table.getSchema().getFields()
				.stream()
				.forEach(f -> schemaObject.add(getNestedRecord(f, "root")));

		return schemaObject.toSchema();
	}

	private AvroRecord getNestedRecord(TableFieldSchema field, String namespace) {
		final Schema.Type type = AvroRecord.getType(field.getType());
		if (AvroRecord.isRecord(type)) {
			final AvroRecord record = AvroRecord.buildAvroRecordByMode(field.getMode(), field.getName(), namespace,
					field.getDescription());
			if (field.getFields() != null) {
				field.getFields().stream()
						.forEach(f -> record.add(getNestedRecord(f, AvroRecord.formatNamespace(namespace, field.getName()))));
			}
			return record;
		} else {
			return AvroField.buildAvroField(field.getMode(), field.getName(), type, field.getDescription());
		}
	}

	public static BigQuerySchemaConverter getInstance() {
		return INSTANCE;
	}

}
