package com.groovy.avro.util.test;

import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import spock.lang.Specification

import com.java.avro.util.JsonGenericRecordReader

public class JsonGenericRecordReaderTest extends Specification {

	def reader = new JsonGenericRecordReader();

	def "should read nested record with not defined nullable fields"() {

		given:
		def schemaString = '''
		{
		    "type": "record",
		    "name": "Root",
		    "fields": [
		        {
		            "name": "field1",
		            "type": [ "long", "null" ]
		        },
		        {
		            "name": "nestedRecord",
		            "type": [
		                {
		                    "type": "record",
		                    "namespace": "root",
		                    "name": "NestedRecord",
		                    "fields": [
		                        {
		                            "name": "nested1",
		                            "type": [ "long", "null" ]
		                        },
		                        {
		                            "name": "nested2",
		                            "type": [ "long", "null" ]
		                        }
		                    ]
		                },
		                "null"
		            ]
		        }
		    ]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "field1" : 10999859003, 
		    "nestedRecord": 
		    { 
		        "nested1" : 123321321
		    }
		}
		'''

		when:
		def record = reader.read(jsonString.getBytes(), schema)

		then:
		record != null
		record.get("field1").equals(10999859003)
		record.get("nestedRecord") != null
	}

	def "should convert the whole record when nullable field is missing"() {

		given:
		def schemaString = '''
		{
		    "type": "record",
		    "name": "Root",
		    "fields": [
		        {
		            "name": "field1",
		            "type": [ "long" ]
		        },
		        {
		            "name": "field2",
		            "type": [ "long", "null" ]
		        }
		    ]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "field1" : 10999859003
		}
		'''

		when:
		def record = reader.read(jsonString.getBytes(), schema)

		then:
		record != null
		record.get("field1").equals(10999859003)
		record.get("field2") == null
	}

	def "should throw exception when converting non-nullable field"() {
		given:

		def schemaString = '''
		{
		    "type": "record",
		    "name": "Root",
		    "fields": [
		        {
		            "name": "field1",
		            "type": [ "long" ]
		        },
		        {
		            "name": "field2",
		            "type": [ "long" ]
		        }
		    ]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "field1" : 10999859003
		}
		'''
		when:
		reader.read(jsonString.getBytes(), schema)

		then:
		def e = thrown AvroRuntimeException
		e.message == 'Failed to convert JSON to Avro'
		e.cause.toString() == 'org.apache.avro.AvroRuntimeException: Field field2 type:UNION pos:1 not set and has no default value'
	}

	def "should convert complex data when given schema with enum arrays empty-arrays numbers"() {
		given:
		def schemaString = this.getClass().getResource('/META-INF/schema/complex-schema.avsc').text
		def schema = new Schema.Parser().parse(schemaString)

		def dataString = this.getClass().getResource('/META-INF/data/complex-data.avro').text

		when:
		def record = reader.read(dataString.getBytes(), schema)

		then:
		record != null
	}

	def "should convert all data format"() {
		given:
		def schemaString = '''
			{
		    "type": "record",
		    "name": "Root",
		    "fields": [
		        {
		            "name": "field1",
		            "type": "double"
		        },
		        {
		            "name": "field2",
		            "type": "boolean"
		        },
				{
					"name": "field3",
					"type": "float"
				},
				{
					"name": "field4",
					"type": "int"
				}, 
				{ 
					"name": "suit",
					"type":
					{
						"name": "Suit",
						"type": "enum",
						"symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
					}
				},
				{
		            "name": "arrayField",
		            "type": {
		                "type": "array",
		                "items": {
		                    "type": "record",
		                    "namespace": "root",
		                    "name": "ArrayField",
		                    "fields": [
		                        {
		                            "name": "index",
		                            "type": [ "long", "null" ]
		                        }
		                    ]
		                }
		            }
		        },
				{
		            "name": "arrayField2",
		            "type": {
		                "type": "array",
		                "items": {
		                    "type": "record",
		                    "namespace": "root",
		                    "name": "ArrayField2",
		                    "fields": [
		                        {
		                            "name": "index2",
		                            "type": [ "long", "null" ]
		                        }
		                    ]
		                }
		            }
		        },
				{
					"name": "nullField",
					"type": [ "long", "null" ]
				},
				{
					"name": "mapField",
					"type": 
					{
						"type": "map",
						"values": "string"
					}
				}
		    ]
			}
			'''
		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "field1" : 109998.566,
			"field2": false,
			"field3": 234.56,
			"field4": 234,
			"suit": "SPADES",
			"arrayField": [
				{"index": 202020},
				{"index": 300000}
			],
			"arrayField2": null,
			"nullField": null,
			"mapField": {
				"firstValue": "value1",
				"secondValue": "value2",
				"thirdValue": "value3"
			}
		}
		'''
		when:
		def GenericRecord record = reader.read(jsonString, schema)

		then:
		record != null

		record.get("field1") != null
		record.get("field1") == 109998.566

		record.get("field2") != null
		record.get("field2") == false

		record.get("field3") != null
		record.get("field3") == 234.56f

		record.get("field4") != null
		record.get("field4") == 234

		record.get("suit") != null
		record.get("suit").toString() == 'SPADES'

		record.get("arrayField") != null
		record.get("arrayField").values == [[202020], [300000]]

		record.get("arrayField2") != null
		record.get("arrayField2") == []
		record.get("arrayField2").isEmpty()

		record.get("mapField") != null
		record.get("mapField") == [
			"firstValue": "value1",
			"secondValue": "value2",
			"thirdValue": "value3"
		]
	}

	def "should throw AvroRuntimeException when incompatible union type"() {
		given:
		def schemaString = '''
		{
		    "type": "record",
		    "name": "Root",
		    "fields": [
		        {
		            "name": "field1",
		            "type": [ "long", "null" ]
		        }
		    ]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "field1" : true
		}
		'''
		when:
		reader.read(jsonString.getBytes(), schema)

		then:
		def e = thrown AvroRuntimeException
		e.message == 'Failed to convert JSON to Avro'
		e.cause.toString() == 'org.apache.avro.AvroTypeException: Could not evaluate union, field field1 is expected to be one of these: LONG, NULL. If this is a complex type, check if offending field: field1 adheres to schema.'
	}

	def "should throw AvroRuntimeException when given empty enum"() {
		given:
		def schemaString = '''
		{
			"name": "Root",
			"type": "record",
			"fields": [
				{
					"name": "suit",
					"type":
					{
						"name": "Suit",
						"type": "enum",
						"symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
					}
				}
			]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "suit" : null
		}
		'''
		when:
		reader.read(jsonString.getBytes(), schema)

		then:
		def e = thrown AvroRuntimeException
		e.message == 'Failed to convert JSON to Avro'
		e.cause.toString() == 'org.apache.avro.AvroTypeException: Field suit is expected to be type: java.lang.String'
	}

	def "should throw AvroRuntimeException when given wrong enum name"() {
		given:
		def schemaString = '''
		{
			"name": "Root",
			"type": "record",
			"fields": [
				{
					"name": "suit",
					"type":
					{
						"name": "Suit",
						"type": "enum",
						"symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
					}
				}
			]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "suit" : "NO_SPADES"
		}
		'''
		when:
		def record = reader.read(jsonString, schema)

		then:
		def e = thrown AvroRuntimeException
		e.message == 'Failed to convert JSON to Avro'
		e.cause.toString() == 'org.apache.avro.AvroTypeException: Field suit is expected to be of enum type and be one of SPADES, HEARTS, DIAMONDS, CLUBS'
	}

	def "should throw exception when converting bad json string to map"() {
		given:
		def schemaString = '''
		{
			"name": "Root",
			"type": "record",
			"fields": [
				{
					"name": "suit",
					"type": "string"
				}
			]
		}
		'''

		def schema = new Schema.Parser().parse(schemaString)

		def jsonString = '''
		{
		    "suit" : 
		}
		'''
		when:
		def record = reader.read(jsonString, schema)

		then:
		def e = thrown AvroRuntimeException
		e.message == 'Failed to parse json to map format.'
		e.cause.toString().contains('com.fasterxml.jackson.core.JsonParseException: Unexpected character (\'}\' (code 125)):')
	}
}