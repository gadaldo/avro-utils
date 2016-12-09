package com.groovy.avro.util.test

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory

import spock.lang.Specification

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableSchema
import com.java.avro.util.AvroUtils

class AvroUtilsTest extends Specification {

	def "should convert the message in a tablerow when message has enum" () {
		given:
		def message = '''
		{
			"memberId": 14425224,
			"version": "2.0",
			"parentVersion": "ONE"
		}
		'''

		def listField = [
			new TableFieldSchema().setName("memberId").setType("INTEGER").setMode("REQUIRED"),
			new TableFieldSchema().setName("version").setType("STRING").setMode("REQUIRED"),
			new TableFieldSchema().setName("parentVersion").setType("STRING").setMode("REQUIRED")
		]

		def tableSchema = new TableSchema().setFields(listField)

		def schemaString = '''{
			"type": "record",
			"name": "PersonalisationEvent",
			"fields": [{
					"name": "memberId",
					"type": "int"
				}, {
					"name": "version",
					"type": "string"
				}, {
					"name": "parentVersion",
					"type": {
						"name": "ParentVersion",
						"type": "enum",
						"symbols": ["ONE", "TWO", "THREE", "FOUR"]
					}
				}
			]
		}
		'''

		def schema  = new Schema.Parser().parse(schemaString)

		def record = getRecord(schema, message)

		when:
		def tableRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		assert tableRow != null
		assert tableRow.get("memberId") != null
		assert tableRow.get("version") != null
		assert tableRow.get("parentVersion") != null
		assert tableRow.get("parentVersion") == "ONE"
	}

	def "should convert the message in a tablerow when message has nullable field" () {
		given:
		def message = '''
		{
			"memberId": 14425224,
			"version": { "string": "2.0" },
			"parentVersion": "ONE"
		}
		'''

		def listField = [
			new TableFieldSchema().setName("memberId").setType("INTEGER").setMode("REQUIRED"),
			new TableFieldSchema().setName("version").setType("STRING").setMode("NULLABLE"),
			new TableFieldSchema().setName("parentVersion").setType("STRING").setMode("REQUIRED")
		]

		def tableSchema = new TableSchema().setFields(listField)

		def schemaString = '''{
			"type": "record",
			"name": "PersonalisationEvent",
			"fields": [{
					"name": "memberId",
					"type": "int"
				}, {
					"name": "version",
					"type": ["null", "string"]
				}, {
					"name": "parentVersion",
					"type": {
						"name": "ParentVersion",
						"type": "enum",
						"symbols": ["ONE", "TWO", "THREE", "FOUR"]
					}
				}
			]
		}
		'''

		def schema  = new Schema.Parser().parse(schemaString)

		def record = getRecord(schema, message)

		when:
		def tableRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		assert tableRow != null
		assert tableRow.get("memberId") != null
		assert tableRow.get("version") != null
		assert tableRow.get("parentVersion") != null
		assert tableRow.get("parentVersion") == "ONE"
		assert tableRow.get("memberId") == 14425224
	}

	def "should convert message in tableRow when message contains timestamp"() {
		given:
		def schemaString = '''{
			"type": "record",
			"name": "PersonalisationEvent",
			"fields": [{
					"name": "date",
					"type": "long"
				}
			]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def message = '''
			{ "date": 1482192000000 }
		'''

		def record = getRecord(schema, message)

		def listField = [
			new TableFieldSchema().setName("date").setType("TIMESTAMP").setMode("REQUIRED")
		]

		def tableSchema = new TableSchema().setFields(listField)

		when:
		def tableRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		assert tableRow != null
		assert tableRow.get("date") != null
		assert tableRow.get("date") == '1970-01-18 03:43:12 UTC'
	}

	def "should convert message in tableRow when message contains repeated values"() {
		given:
		def schemaString = '''{
			"type": "record",
			"name": "PersonalisationEvent",
			"fields": [
				{ 
					"name": "repeatedRecord",
				 	"type": {
						"type": "array",
				  		"items": "string"
					}
				}
			]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def message = '''
			{ "repeatedRecord": ["arrayValue"] }
		'''

		def record = getRecord(schema, message)

		def listField = [
			new TableFieldSchema().setName("repeatedRecord").setType("STRING").setMode("REPEATED")
		]

		def tableSchema = new TableSchema().setFields(listField)

		when:
		def tableRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		assert tableRow != null
		assert tableRow.get("repeatedRecord") != null
		assert tableRow.get("repeatedRecord") instanceof List
		assert tableRow.get("repeatedRecord") == ["arrayValue"]
	}

	def "should convert message to tableRow when message contains boolean, float, record, bytes, date, datetime and time"() {
		given:
		def schemaString = '''{
			"type": "record",
			"name": "PersonalisationEvent",
			"fields": [ {
					"name": "floatField",
					"type": "float"
				}, {
					"name": "booleanField",
					"type": "boolean"
				},{
					"name": "bytesField",
					"type": "bytes"
				}, {
					"name": "dateField",
					"type": "string"
				}, {
					"name": "dateTimeField",
					"type": "string"
				}, {
					"name": "timeField",
					"type": "string"
				}, {
					"name": "recordField",
					"type": {
						"name": "RecordField",
						"type": "record",
						"fields": [ {
							"name": "field1",
							"type": "double"
						} ]
					}
				}
			]
		}
		'''

		def message = '''{
			"floatField": 2.456,
			"booleanField": true,
			"bytesField": "97, 32, 115, 116, 114, 105, 110, 103, 32, 97, 115, 32, 98, 121, 116, 101, 32, 97, 114, 114, 97, 121",
			"dateField": "20/12/2012",
			"dateTimeField": "03/04/2015 12:15:59",
			"timeField": "12:34:56",
			"recordField": {
				"field1": 123654367.564738
			}
		}
		'''
		def schema = new Schema.Parser().parse(schemaString)

		def record = getRecord(schema, message)

		def listField = [
			new TableFieldSchema().setName("floatField").setType("FLOAT").setMode("REQUIRED"),
			new TableFieldSchema().setName("booleanField").setType("BOOLEAN").setMode("REQUIRED"),
			new TableFieldSchema().setName("bytesField").setType("BYTES").setMode("REQUIRED"),
			new TableFieldSchema().setName("dateField").setType("DATE").setMode("REQUIRED"),
			new TableFieldSchema().setName("dateTimeField").setType("DATETIME").setMode("REQUIRED"),
			new TableFieldSchema().setName("timeField").setType("TIME").setMode("REQUIRED"),
			new TableFieldSchema().setName("recordField").setType("RECORD").setMode("REQUIRED").setFields([
				new TableFieldSchema().setName("field1").setType("FLOAT").setMode("REQUIRED")
			])
		]

		def tableSchema = new TableSchema().setFields(listField)

		when:
		def tableRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		assert tableRow != null
		assert tableRow.get("floatField") != null
		assert tableRow.get("floatField") == 2.456f
		assert tableRow.get("booleanField") != null
		assert tableRow.get("booleanField") == true
		assert tableRow.get("bytesField") != null
		assert tableRow.get("bytesField") instanceof String
		assert tableRow.get("bytesField") == "OTcsIDMyLCAxMTUsIDExNiwgMTE0LCAxMDUsIDExMCwgMTAzLCAzMiwgOTcsIDExNSwgMzIsIDk4LCAxMjEsIDExNiwgMTAxLCAzMiwgOTcsIDExNCwgMTE0LCA5NywgMTIx"
		assert tableRow.get("dateField") != null
		assert tableRow.get("dateField") instanceof String
		assert tableRow.get("dateTimeField") != null
		assert tableRow.get("dateTimeField") instanceof String
		assert tableRow.get("timeField") != null
		assert tableRow.get("timeField") instanceof String
		assert tableRow.get("recordField") != null
		assert tableRow.get("recordField") instanceof Map
		assert tableRow.get("recordField").get("field1") != null
		assert tableRow.get("recordField").get("field1") instanceof Double
		assert tableRow.get("recordField").get("field1") == 123654367.564738
	}

	def "should throw exception when schema contains unsupported field type"() {
		given:
		def schemaString = '''{
			"name": "record1", 
			"type": "record",
			"fields": [ {
				"name": "notManagedField", 
				"type": "long" 
			} ]
		}'''

		def schema = new Schema.Parser().parse(schemaString)

		def listField = [
			new TableFieldSchema().setName("notManagedField").setType("UNEXPECTED").setMode("REQUIRED")
		]

		def tableSchema = new TableSchema().setFields(listField)

		def message = '''{ "notManagedField": 122222 }'''

		def record = getRecord(schema, message)

		when:
		AvroUtils.convertGenericRecordToTableRow(record, tableSchema)

		then:
		def e = thrown RuntimeException
		e.message == 'Unsupported BigQuery type: UNEXPECTED'
	}

	private def getRecord(Schema schema, String data) {
		final GenericDatumReader<String> reader = new GenericDatumReader<>(schema)

		final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, data)

		return reader.read(null, decoder)
	}
}
