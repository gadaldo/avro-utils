package com.java.avro.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AvroSchema object being built from {@link BigQuerySchemaConverter#toAvroSchema(com.google.api.services.bigquery.model.Table)}. <br>
 * It's internal class contains a list of fields and record. It builds the {@link Schema} object when {@link AvroRecord#toSchema()} method has called.
 * <br>
 * 
 * <br>
 * <strong>Note:</strong> this object is hidden from outside the package. <br>
 * <strong>Note:</strong> this object only supports types from Bigquery schema source.
 * 
 * @author giuseppe.adaldo
 *
 */
class AvroSchemaObject {

	protected static final Logger LOG = LoggerFactory.getLogger(AvroSchemaObject.class);

	private String name;
	private List<AvroRecord> fields;
	private String doc;

	private AvroSchemaObject() {
	}

	static final AvroSchemaObject build(String name, String doc) {
		final AvroSchemaObject schema = new AvroSchemaObject();
		schema.fields = new ArrayList<>();
		schema.name = name;
		schema.doc = doc != null && doc.trim().isEmpty() ? null : doc;
		return schema;
	}

	AvroSchemaObject add(AvroRecord r) {
		this.fields.add(r);
		return this;
	}

	public Schema toSchema() {
		final List<Schema.Field> fields = this.fields.stream()
				.map(f -> f.toField())
				.filter(s -> s != null)
				.collect(Collectors.toList());

		return Schema.createRecord(name, doc, "root", false, fields);
	}

	/**
	 * Generic Avro record representation. A schema field can itself contains other fields, in that case it is a record. Bigquery record can be
	 * 'nullable' or 'repeated' type only, so no map support is needed. Check out this information here:
	 * {@link: https://cloud.google.com/bigquery/docs/data#nested}
	 * 
	 * @author giuseppe.adaldo
	 *
	 */
	abstract static class AvroRecord {
		private static final String TABLE_SCHEMA_REPEATED_RECORD = "REPEATED";

		protected String name;
		protected String namespace;
		protected String doc;
		protected List<AvroRecord> fields;

		private AvroRecord() {
		}

		public AvroRecord add(AvroRecord r) {
			this.fields.add(r);
			return this;
		}

		static AvroRecord buildAvroRecordByMode(String mode, String name, String namespace, String doc) {
			if (TABLE_SCHEMA_REPEATED_RECORD.equalsIgnoreCase(mode))
				return RepeatableRecord.build(name, namespace, doc);
			return NullableRecord.build(name, namespace, doc);
		}

		static boolean isRecord(Type type) {
			return Type.RECORD.equals(type);
		}

		/**
		 * Returns the avro type for the relative Bigquery schema value. <br>
		 * See also {@link https://cloud.google.com/bigquery/docs/reference/rest/v2/tables},
		 * {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types}, {@link https://cloud.google.com/bigquery/data-types} and
		 * {@link https://cloud.google.com/bigquery/data-formats} for more information about Bigquery types and SQL date/time representation. <br>
		 * 
		 * @param type string type
		 * @return the avro schema type
		 */
		static Type getType(String type) {
			switch (type) {
			case "INTEGER":
			case "LONG":
				return Type.LONG;
			case "DOUBLE":
				return Type.DOUBLE;
			case "FLOAT":
				return Type.FLOAT;
			case "BOOLEAN":
				return Type.BOOLEAN;
			case "STRING":
				return Type.STRING;
			case "RECORD":
				return Type.RECORD;
			case "BYTES":
				return Type.BYTES;
			case "TIMESTAMP":
			case "TIME":
			case "DATETIME":
			case "DATE":
				return Type.STRING;
			case "NULL":
				return Type.NULL;
			}
			throw new AvroRuntimeException("Not valid avro type for the big query type: '" + type + "'");
		}

		protected static final String getCapitalString(String name) {
			return Character.toUpperCase(name.charAt(0)) + name.substring(1);
		}

		static String formatNamespace(String base, String child) {
			return String.format("%s.%s", base, child);
		}

		protected abstract Schema.Field toField();

	}

	/**
	 * Nullable Avro record representation. It could be either a record type object with array of fields or null.
	 * 
	 * @author giuseppe.adaldo
	 *
	 */
	static final class NullableRecord extends AvroRecord {
		private NullableRecord() {
		}

		private static final NullableRecord build(String name, String namespace, String doc) {
			final NullableRecord record = new NullableRecord();
			record.name = name;
			record.doc = doc;
			record.namespace = namespace.toLowerCase();
			record.fields = new ArrayList<>();
			return record;
		}

		protected Schema getSchema() {
			return Schema.createUnion(
					Schema.createRecord(getCapitalString(name), doc, namespace, false,
							this.fields.stream()
									.map(field -> field.toField())
									.collect(Collectors.toList())),
					Schema.create(Type.NULL));
		}

		@Override
		protected Field toField() {
			return new Schema.Field(name, getSchema(), doc, (Object) null);
		}
	}

	/**
	 * Repeatable Avro record representation. It's a non-nullable array type object with items in it. Each item is a record that contains a name,
	 * namespace definition and array of fields.
	 * 
	 * @author giuseppe.adaldo
	 *
	 */
	static final class RepeatableRecord extends AvroRecord {
		private RepeatableRecord() {
		}

		private static final RepeatableRecord build(String name, String namespace, String doc) {
			final RepeatableRecord record = new RepeatableRecord();
			record.name = name;
			record.namespace = namespace.toLowerCase();
			record.fields = new ArrayList<>();
			return record;
		}

		protected Schema getSchema() {
			final Schema recordSchema = Schema.createRecord(getCapitalString(name), doc, namespace, false,
					this.fields.stream()
							.map(f -> f.toField())
							.collect(Collectors.toList()));

			return Schema.createArray(recordSchema);
		}

		@Override
		protected Field toField() {
			return new Schema.Field(name, getSchema(), doc, (Object) null);
		}
	}

	/**
	 * Generic Avro field representation. Is the leaf in the tree of the schema object. It can be Nullable or simple.
	 * 
	 * @author giuseppe.adaldo
	 *
	 */
	static abstract class AvroField extends AvroRecord {

		private static final String TABLE_SCHEMA_NULLABLE = "NULLABLE";

		protected Type type;

		static AvroField buildAvroField(String mode, String name, Type type, String doc) {
			if (TABLE_SCHEMA_NULLABLE.equals(mode))
				return NullableAvroField.build(name, type, doc);
			else
				return SimpleAvroField.build(name, type, doc);
		}

		private static List<Schema> getUnion(Type type) {
			return Arrays.<Schema>asList(new Schema[] { Schema.create(type), Schema.create(Type.NULL) });
		}

		/**
		 * Simple field representation: just a field with name and type.
		 * 
		 * @author giuseppe.adaldo
		 *
		 */
		static final class SimpleAvroField extends AvroField {
			private SimpleAvroField() {
			}

			static SimpleAvroField build(String name, Type type, String doc) {
				final SimpleAvroField field = new SimpleAvroField();
				field.name = name;
				field.type = type;
				return field;
			}

			@Override
			protected Schema.Field toField() {
				return new Schema.Field(name, Schema.create(type), doc, (Object) null);
			}
		}

		/**
		 * It's a simple Avro field that can be null in case of no-value.
		 * 
		 * @author giuseppe.adaldo
		 *
		 */
		static final class NullableAvroField extends AvroField {
			private NullableAvroField() {
			}

			static NullableAvroField build(String name, Type type, String doc) {
				final NullableAvroField field = new NullableAvroField();
				field.name = name;
				field.type = type;
				return field;
			}

			@Override
			protected Schema.Field toField() {
				return new Schema.Field(name, Schema.createUnion(getUnion(type)), doc, (Object) null);
			}
		}
	}

}
