package com.java.avro.util;

import static com.java.avro.util.AvroTypeExceptions.enumException;
import static com.java.avro.util.AvroTypeExceptions.typeException;
import static com.java.avro.util.AvroTypeExceptions.unionException;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;

/**
 * Reads a json string or a map converting in a {@link GenericRecord}. <br>
 * It's also usable when transforming a {@link TableRow} data to a {@link GenericRecord} skipping errors such us missing value field when nullable or
 * empty arrays.
 * 
 * @author giuseppe.adaldo
 *
 */
public class JsonGenericRecordReader {

	private static final Object INCOMPATIBLE = new Object();

	private final ObjectMapper mapper;

	public JsonGenericRecordReader() {
		this.mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
	}

	public GenericData.Record read(String data, Schema schema) {
		return read(data.getBytes(StandardCharsets.UTF_8), schema);
	}

	@SuppressWarnings("unchecked")
	public GenericData.Record read(byte[] data, Schema schema) {
		try {
			return read(mapper.readValue(data, Map.class), schema);
		} catch (IOException ex) {
			throw new AvroRuntimeException("Failed to parse json to map format.", ex);
		}
	}

	public GenericData.Record read(Map<String, Object> json, Schema schema) {
		Deque<String> path = new ArrayDeque<>();
		try {
			return readRecord(json, schema, path);
		} catch (AvroRuntimeException ex) {
			throw new AvroRuntimeException("Failed to convert JSON to Avro", ex);
		}
	}

	private GenericRecordBuilder getEnrichedRecordOnError(Schema schema, GenericRecordBuilder record) {
		schema.getFields().stream()
				.filter(f -> record.get(f) == null && f.schema().getType().equals(Type.UNION) && isNullable(f))
				.forEach(f -> record.set(f, null));
		return record;
	}

	private boolean isNullable(Field field) {
		return field.schema().getTypes().stream()
				.filter(s -> s.getType().equals(Type.NULL))
				.findAny()
				.isPresent();
	}

	private GenericData.Record readRecord(Map<String, Object> json, Schema schema, Deque<String> path) {
		GenericRecordBuilder record = new GenericRecordBuilder(schema);
		json.entrySet().forEach(entry -> ofNullable(schema.getField(entry.getKey()))
				.ifPresent(field -> record.set(field, read(field, field.schema(), entry.getValue(), path, false))));
		try {
			return record.build();
		} catch (AvroRuntimeException e) {
			return getEnrichedRecordOnError(schema, record).build();
		}
	}

	@SuppressWarnings("unchecked")
	private Object read(Schema.Field field, Schema schema, Object value, Deque<String> path, boolean silently) {
		boolean pushed = !field.name().equals(path.peek());
		if (pushed) {
			path.push(field.name());
		}
		Object result;

		switch (schema.getType()) {
		case RECORD:
			result = onValidType(value, Map.class, path, silently, map -> readRecord(map, schema, path));
			break;
		case ARRAY:
			if (value == null)
				value = Collections.EMPTY_LIST;
			result = onValidType(value, List.class, path, silently, list -> readArray(field, schema, list, path));
			break;
		case MAP:
			result = onValidType(value, Map.class, path, silently, map -> readMap(field, schema, map, path));
			break;
		case UNION:
			result = readUnion(field, schema, value, path);
			break;
		case INT:
			result = onValidNumber(value, path, silently, Number::intValue, Integer::parseInt);
			break;
		case LONG:
			result = onValidNumber(value, path, silently, Number::longValue, Long::parseLong);
			break;
		case FLOAT:
			result = onValidNumber(value, path, silently, Number::floatValue, Float::parseFloat);
			break;
		case DOUBLE:
			result = onValidNumber(value, path, silently, Number::doubleValue, Double::parseDouble);
			break;
		case BOOLEAN:
			result = onValidType(value, Boolean.class, path, silently, bool -> bool);
			break;
		case ENUM:
			result = onValidType(value, String.class, path, silently, string -> ensureEnum(schema, string, path));
			break;
		case STRING:
			result = onValidType(value, String.class, path, silently, string -> string);
			break;
		case NULL:
			result = value == null ? value : INCOMPATIBLE;
			break;
		default:
			throw new AvroTypeException("Unsupported type: " + field.schema().getType());
		}

		if (pushed) {
			path.pop();
		}
		return result;
	}

	private List<Object> readArray(Schema.Field field, Schema schema, List<Object> items, Deque<String> path) {
		return items.stream().map(item -> read(field, schema.getElementType(), item, path, false)).collect(toList());
	}

	private Map<String, Object> readMap(Schema.Field field, Schema schema, Map<String, Object> map, Deque<String> path) {
		return map.entrySet()
				.stream()
				.collect(toMap(
						Map.Entry::getKey, entry -> read(field, schema.getValueType(), entry.getValue(), path, false)));
	}

	private Object readUnion(Schema.Field field, Schema schema, Object value, Deque<String> path) {
		List<Schema> types = schema.getTypes();
		for (Schema type : types) {
			try {
				Object nestedValue = read(field, type, value, path, true);
				if (nestedValue == INCOMPATIBLE) {
					continue;
				} else {
					return nestedValue;
				}
			} catch (AvroRuntimeException e) {
				// thrown only for union of more complex types like records
				continue;
			}
		}
		throw unionException(
				field.name(),
				types.stream().map(Schema::getType).map(Object::toString).collect(joining(", ")),
				path);
	}

	private Object ensureEnum(Schema schema, Object value, Deque<String> path) {
		List<String> symbols = schema.getEnumSymbols();
		if (symbols.contains(value)) {
			return new GenericData.EnumSymbol(schema, (String) value);
		}
		throw enumException(path, symbols.stream().map(String::valueOf).collect(joining(", ")));
	}

	@SuppressWarnings("unchecked")
	public <T> Object onValidType(Object value, Class<T> type, Deque<String> path, boolean silently, Function<T, Object> function)
			throws AvroTypeException {

		if (type.isInstance(value)) {
			return function.apply((T) value);
		} else {
			if (silently) {
				return INCOMPATIBLE;
			} else {
				throw typeException(path, type.getTypeName());
			}
		}
	}

	public Object onValidNumber(Object value, Deque<String> path, boolean silently, Function<Number, Object> function, Function<String, Number> fn) {
		if (String.class.isInstance(value))
			value = fn.apply(value.toString());
		return onValidType(value, Number.class, path, silently, function);
	}

}
