package com.java.avro.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author giuseppe.adaldo
 *
 */
interface AvroSchema {

	default String format(Object... args) {
		return String.format(getFormatter(), args);
	}

	String getFormatter();

	static class AvroSchemaObject implements AvroSchema {

		private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);

		private static final String FORMATTER = "{\"name\": \"%s\", \"type\": \"record\", \"fields\": [%s]}";

		private String name;
		private List<AvroRecord> records;

		private AvroSchemaObject() {
		}

		static final AvroSchemaObject build(String name, List<AvroRecord> records) {
			final AvroSchemaObject schema = new AvroSchemaObject();
			schema.name = name;
			schema.records = records;
			return schema;
		}

		static final AvroSchemaObject build(String name) {
			return build(name, new ArrayList<>());
		}

		@Override
		public String toString() {
			final StringBuilder schemaString = new StringBuilder();
			final StringBuilder fields = new StringBuilder();
			this.records.stream().forEach(f -> fields.append(f.toString()));
			schemaString.append(this.format(name, AvroRecord.removeLastObjectSeparator(fields.toString())));
			return schemaString.toString();
		}

		@Override
		public String getFormatter() {
			return FORMATTER;
		}

		AvroSchemaObject add(AvroRecord r) {
			this.records.add(r);
			return this;
		}

		public Schema toSchema() {
			final String schemaString = this.toString();
			LOG.debug(schemaString);
			return new Schema.Parser().parse(schemaString);
		}

		/**
		 * Generic Avro record representation.
		 * 
		 * @author giuseppe.adaldo
		 *
		 */
		abstract static class AvroRecord implements AvroSchema {
			private static final String TABLE_SCHEMA_INT_TYPE = "INTEGER";

			private static final String AVRO_SCHEMA_INT_TYPE = "long";

			private static final String TABLE_SCHEMA_RECORD_TYPE = "record";

			private static final String AVRO_SCHEMA_EMBEDDED_SEPARATOR = ".";

			private static final String TABLE_SCHEMA_REPEATED_RECORD = "REPEATED";

			protected String name;
			protected String type;
			protected String namespace;
			protected List<AvroRecord> fields;

			private AvroRecord() {
			}

			public AvroRecord add(AvroRecord r) {
				this.fields.add(r);
				return this;
			}

			static AvroRecord buildAvroRecordByMode(String mode, String name, String type, String namespace) {
				if (TABLE_SCHEMA_REPEATED_RECORD.equalsIgnoreCase(mode))
					return RepeatableRecord.build(name, type, namespace);
				return NullableRecord.build(name, namespace);
			}

			static boolean isRecord(String type) {
				return TABLE_SCHEMA_RECORD_TYPE.equalsIgnoreCase(type);
			}

			static String getType(String type) {
				switch (type) {
				case TABLE_SCHEMA_INT_TYPE:
					return AVRO_SCHEMA_INT_TYPE;

				default:
					return type.toLowerCase();
				}
			}

			static String buildNamespace(String base, String field) {
				return base + AVRO_SCHEMA_EMBEDDED_SEPARATOR + field.toLowerCase();
			}

			protected static final String getCapitalString(String name) {
				return Character.toUpperCase(name.charAt(0)) + name.substring(1);
			}

			protected static String removeLastObjectSeparator(String json) {
				if (json.trim().isEmpty())
					return json;
				return json.substring(0, json.toString().lastIndexOf(","));
			}

			protected static String getNamespace(String namespace) {
				return namespace.contains(".") ? namespace.substring(0, namespace.lastIndexOf(".")) : namespace;
			}
		}

		/**
		 * Nullable Avro record representation. It could be either a record type object with array of fields or null.
		 * 
		 * @author giuseppe.adaldo
		 *
		 */
		static final class NullableRecord extends AvroRecord {
			private static final String FORMATTER = "{\"name\": \"%s\", \"type\": [{\"type\": \"record\", \"name\": \"%s\", \"namespace\": \"%s\", \"fields\": [%s]}, \"null\"]},";

			private NullableRecord() {
			}

			private static final NullableRecord build(String name, String namespace) {
				final NullableRecord record = new NullableRecord();
				record.name = name;
				record.namespace = namespace;
				record.fields = new ArrayList<>();
				return record;
			}

			@Override
			public String toString() {
				final String capitalName = getCapitalString(name);
				final StringBuilder sb = new StringBuilder();
				final StringBuilder fields = new StringBuilder();
				this.fields.stream().forEach(f -> fields.append(f.toString()));
				sb.append(format(name, capitalName, getNamespace(namespace), removeLastObjectSeparator(fields.toString())));
				return sb.toString();
			}

			@Override
			public String getFormatter() {
				return FORMATTER;
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
			private static final String FORMATTER = "{\"name\": \"%s\", \"type\": {\"type\": \"array\", \"items\": { \"type\": \"record\", \"name\":\"%s\", \"namespace\":\"%s\", \"fields\": [%s]}}},";

			private RepeatableRecord() {
			}

			private static final RepeatableRecord build(String name, String type, String namespace) {
				final RepeatableRecord record = new RepeatableRecord();
				record.name = name;
				record.namespace = namespace;
				record.fields = new ArrayList<>();
				return record;
			}

			@Override
			public String toString() {
				final StringBuilder sb = new StringBuilder();
				final StringBuilder fields = new StringBuilder();
				this.fields.stream().forEach(f -> fields.append(f.toString()));
				sb.append(format(name, getCapitalString(name), getNamespace(namespace), removeLastObjectSeparator(fields.toString())));
				return sb.toString();
			}

			@Override
			public String getFormatter() {
				return FORMATTER;
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

			static AvroField buildAvroField(String mode, String name, String type) {
				if (TABLE_SCHEMA_NULLABLE.equals(mode))
					return NullableAvroField.build(name, type);
				else
					return SimpleAvroField.build(name, type);
			}

			/**
			 * Simple field representation: just a field with name and type.
			 * 
			 * @author giuseppe.adaldo
			 *
			 */
			public static final class SimpleAvroField extends AvroField {
				private static final String FORMATTER = "{\"name\": \"%s\", \"type\": \"%s\"},";

				private SimpleAvroField() {
				}

				public static SimpleAvroField build(String name, String type) {
					final SimpleAvroField field = new SimpleAvroField();
					field.name = name;
					field.type = type;
					return field;
				}

				@Override
				public String toString() {
					return format(name, type);
				}

				@Override
				public String getFormatter() {
					return FORMATTER;
				}
			}

			/**
			 * It's a simple Avro field that can be null in case of no-value.
			 * 
			 * @author giuseppe.adaldo
			 *
			 */
			static final class NullableAvroField extends AvroField {
				private static final String FORMATTER = "{\"name\": \"%s\", \"type\": [\"%s\", \"null\"]},";

				private NullableAvroField() {
				}

				static NullableAvroField build(String name, String type) {
					final NullableAvroField field = new NullableAvroField();
					field.name = name;
					field.type = type;
					return field;
				}

				@Override
				public String toString() {
					return format(name, type);
				}

				@Override
				public String getFormatter() {
					return FORMATTER;
				}
			}
		}
	}

}
