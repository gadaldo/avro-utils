package com.java.avro.util;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.avro.Schema;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.java.avro.util.AvroSchema.AvroSchemaObject;
import com.java.avro.util.AvroSchema.AvroSchemaObject.AvroField;
import com.java.avro.util.AvroSchema.AvroSchemaObject.AvroRecord;

/**
 * Utility to transform a BigQuery table schema to an AVRO schema.
 * 
 * @author giuseppe.adaldo
 *
 */
public enum BigQueryToAvroSchema {

	INSTANCE;

	/**
	 * Returns the {@link Schema} representation of the given Bigquery {@link Table} object.
	 * 
	 * @param table
	 * @return
	 * @throws IOException
	 * @throws GeneralSecurityException
	 */
	public Schema buildSchema(Table table) throws IOException, GeneralSecurityException {
		final AvroSchemaObject schemaObject = AvroSchemaObject.build("Root");
		table.getSchema().getFields()
				.stream()
				.forEach(f -> schemaObject.add(getNestedRecord(f, "root")));

		return schemaObject.toSchema();
	}

	private AvroRecord getNestedRecord(TableFieldSchema field, String namespace) {
		final String type = AvroRecord.getType(field.getType());
		if (AvroRecord.isRecord(type)) {
			final String localNamespace = AvroRecord.buildNamespace(namespace, field.getName());

			final AvroRecord record = AvroRecord.buildAvroRecordByMode(field.getMode(), field.getName(), type, localNamespace);
			if (field.getFields() != null) {
				field.getFields().stream()
						.forEach(f -> record.add(getNestedRecord(f, localNamespace)));
			}
			return record;
		} else {
			return AvroField.buildAvroField(field.getMode(), field.getName(), type);
		}
	}

	public static BigQueryToAvroSchema getInstance() {
		return INSTANCE;
	}

}
