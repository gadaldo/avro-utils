package com.groovy.avro.util.test

import org.apache.avro.Schema

import spock.lang.Specification

import com.java.avro.util.AvroSchemaConverter

class AvroSchemaConverterTest extends Specification {

	def "should convert avro schema into table schema"() {
		given:
		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "simpleSchema",
				"type": "record",
				"fields": [ {
					"name": "intField",
					"type": "int"
				}, {
					"name": "longField",
					"type": "long"
				}, {
					"name": "floatField",
					"type": "float"
				}, {
					"name": "doubleField",
					"type": "double"
				}, {
					"name": "byteField",
					"type": "bytes"
				}, {
					"name": "stringField",
					"type": "string"
				}, {
					"name": "booleanField",
					"type": "boolean"
				}, {
					"name": "unionField",
					"type": ["string", "null"]
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema)

		then:
		assert tableSchema != null
		assert tableSchema.fields.get(0) != null
		assert tableSchema.fields.get(0).name == 'intField'
		assert tableSchema.fields.get(0).mode == 'REQUIRED'
		assert tableSchema.fields.get(1) != null
		assert tableSchema.fields.get(1).name == 'longField'
		assert tableSchema.fields.get(2) != null
		assert tableSchema.fields.get(2).name == 'floatField'
		assert tableSchema.fields.get(2).type == 'FLOAT'
		assert tableSchema.fields.get(3) != null
		assert tableSchema.fields.get(3).name == 'doubleField'
		assert tableSchema.fields.get(3).type == 'FLOAT'
		assert tableSchema.fields.get(4) != null
		assert tableSchema.fields.get(4).name == 'byteField'
		assert tableSchema.fields.get(5) != null
		assert tableSchema.fields.get(5).name == 'stringField'
		assert tableSchema.fields.get(6) != null
		assert tableSchema.fields.get(6).name == 'booleanField'
		assert tableSchema.fields.get(7) != null
		assert tableSchema.fields.get(7).name == 'unionField'
		assert tableSchema.fields.get(7).mode == 'NULLABLE'
	}

	def "should convert avro schema with internal record"() {
		given:
		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "simpleSchema",
				"type": "record",
				"fields": [ {
					"name": "intField",
					"type": "int"
				}, {
					"name": "recordField",
					"type": [ {
						"type": "record",
						"name": "RecordField",
						"fields": [ {
							"name": "internalField",
							"type": "float"
							}, {
								"name": "internalNullableField",
								"type": ["bytes", "null"]
							}, {
								"name": "internalNullableRecord",
								"type": {
									"type": "record",
									"name": "InternalNullableRecord",
									"fields": [ {
										"name": "internalField1",
										"type": ["string", "null"]
									} ]
								}
							} ]
					}, "null"]
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema)

		then:
		assert tableSchema != null
		assert tableSchema.fields.get(0) != null
		assert tableSchema.fields.get(0).name == 'intField'
		assert tableSchema.fields.get(1) != null
		assert tableSchema.fields.get(1).name == 'recordField'
		assert tableSchema.fields.get(1).type == 'RECORD'
		assert tableSchema.fields.get(1).mode == 'NULLABLE'

		assert tableSchema.fields.get(1).fields != null

		def internalField = tableSchema.fields.get(1).fields.get(0)
		assert internalField != null
		assert internalField.name == 'internalField'
		assert internalField.type == 'FLOAT'
		assert internalField.mode == 'REQUIRED'

		def internalNullableField = tableSchema.fields.get(1).fields.get(1)
		assert internalNullableField != null
		assert internalNullableField.name == 'internalNullableField'
		assert internalNullableField.type == 'BYTES'
		assert internalNullableField.mode == 'NULLABLE'

		def internalNullableRecord = tableSchema.fields.get(1).fields.get(2)
		assert internalNullableRecord != null
		assert internalNullableRecord.name == 'internalNullableRecord'
		assert internalNullableRecord.type == 'RECORD'
		assert internalNullableRecord.mode == 'REQUIRED'

		def internalField1 = tableSchema.fields.get(1).fields.get(2).fields.get(0)
		assert internalField1 != null
		assert internalField1.name == 'internalField1'
		assert internalField1.type == 'STRING'
		assert internalField1.mode == 'NULLABLE'
	}

	def "should convert avro schema with array and nullable array" () {
		given:

		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "schemaWithArrays",
				"type": "record",
				"fields": [ {
					"name": "arrayField",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "ArrayField",
							"fields": [ {
									"name": "internalArrayField1",
									"type": "long"
								}, {
									"name": "internalArrayField2",
									"type": "int"
								}
						] }
					}
				}, {
					"name": "arrayField2",
					"type": [ {
						"type": "array",
						"items": {
							"name": "ArrayField2",
							"type": "record",
							"fields": [ {
								"name": "internalArrayField3",
								"type": "long"
							} ]
						}
					}, "null" ]	
				}, {
					"name": "arrayField3",
					"type": { 
						"type": "array",
						"items": "long"
					}
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema)

		then:
		assert tableSchema != null
		assert tableSchema.fields != null

		def arrayField = tableSchema.fields.get(0)
		assert arrayField  != null
		assert arrayField.name == 'arrayField'
		assert arrayField.type == 'RECORD'
		assert arrayField.mode == 'REPEATED'
		assert arrayField.fields != null

		def internalArrayField1 = arrayField.fields.get(0)
		assert internalArrayField1 != null
		assert internalArrayField1.name == 'internalArrayField1'
		assert internalArrayField1.type == 'INTEGER'

		def internalArrayField2 = arrayField.fields.get(1)
		assert internalArrayField2 != null
		assert internalArrayField2.name == 'internalArrayField2'
		assert internalArrayField2.type == 'INTEGER'
		assert internalArrayField2.mode == 'REQUIRED'

		def arrayField2 = tableSchema.fields.get(1)
		assert arrayField2 != null
		assert arrayField2.name == 'arrayField2'
		assert arrayField2.type == 'RECORD'
		assert arrayField2.mode == 'NULLABLE'
		assert arrayField2.fields != null

		def internalArrayField3 = arrayField2.fields.get(0)
		assert internalArrayField3 != null
		assert internalArrayField3.name == 'internalArrayField3'
		assert internalArrayField3.type == 'INTEGER'
		assert internalArrayField3.mode == 'REQUIRED'

		def arrayField3 = tableSchema.fields.get(2)
		assert arrayField3 != null
		assert arrayField3.name == 'arrayField3'
		assert arrayField3.type == 'RECORD'
		assert arrayField3.mode == 'REPEATED'
		assert arrayField3.fields == null
	}

	def "should skip internal fields from record and from arrays" () {
		given:

		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "schemaWithArrays",
				"type": "record",
				"fields": [ {
					"name": "arrayField",
					"type": {
						"type": "array",
						"items": {
							"type": "record",
							"name": "ArrayField",
							"fields": [ {
									"name": "internalArrayField1",
									"type": "long"
								}, {
									"name": "internalArrayField2",
									"type": "int"
								}
						] }
					}
				}, {
					"name": "arrayField2",
					"type": [ {
						"type": "array",
						"items": {
							"name": "ArrayField2",
							"type": "record",
							"fields": [ {
								"name": "internalArrayField3",
								"type": "long"
							} ]
						}
					}, "null" ]	
				}, {
					"name": "arrayField3",
					"type": { 
						"type": "array",
						"items": "long"
					}
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema, 'internalArrayField3','internalArrayField2')

		then:
		assert tableSchema != null
		assert tableSchema.fields != null

		def arrayField = tableSchema.fields.get(0)
		assert arrayField  != null
		assert arrayField.name == 'arrayField'
		assert arrayField.type == 'RECORD'
		assert arrayField.mode == 'REPEATED'
		assert arrayField.fields != null
		assert arrayField.fields.size() == 1

		def internalArrayField1 = arrayField.fields.get(0)
		assert internalArrayField1 != null
		assert internalArrayField1.name == 'internalArrayField1'
		assert internalArrayField1.type == 'INTEGER'

		def arrayField2 = tableSchema.fields.get(1)
		assert arrayField2 != null
		assert arrayField2.name == 'arrayField2'
		assert arrayField2.type == 'RECORD'
		assert arrayField2.mode == 'NULLABLE'
		assert arrayField2.fields == null

		def arrayField3 = tableSchema.fields.get(2)
		assert arrayField3 != null
		assert arrayField3.name == 'arrayField3'
		assert arrayField3.type == 'RECORD'
		assert arrayField3.mode == 'REPEATED'
		assert arrayField3.fields == null
	}

	def "should not skip any field when retrieving Bigquery schema with null" () {
		given:
		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "simpleSchema",
				"type": "record",
				"fields": [ {
					"name": "intField",
					"type": "int"
				}, {
					"name": "recordField",
					"type": [ {
						"type": "record",
						"name": "RecordField",
						"fields": [ {
							"name": "internalField",
							"type": "float"
							} 
						]
					} ]
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema, null)

		then:
		assert tableSchema != null
		assert tableSchema.fields.get(0) != null
		assert tableSchema.fields.get(0).name == 'intField'
		assert tableSchema.fields.get(1) != null
		assert tableSchema.fields.get(1).name == 'recordField'
		assert tableSchema.fields.get(1).type == 'RECORD'
		assert tableSchema.fields.get(1).mode == 'REQUIRED'

		assert tableSchema.fields.get(1).fields != null

		def internalField = tableSchema.fields.get(1).fields.get(0)
		assert internalField != null
		assert internalField.name == 'internalField'
		assert internalField.type == 'FLOAT'
		assert internalField.mode == 'REQUIRED'

		def internalNullableRecord = tableSchema.fields.get(1).fields.get(0)
		assert internalNullableRecord != null
		assert internalNullableRecord.name == 'internalField'
		assert internalNullableRecord.type == 'FLOAT'
		assert internalNullableRecord.mode == 'REQUIRED'
	}


	def "should not skip any field when given a non existing field" () {
		given:
		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "simpleSchema",
				"type": "record",
				"fields": [ {
					"name": "intField",
					"type": "int"
				}, {
					"name": "longField",
					"type": "long"
				}, {
					"name": "floatField",
					"type": "float"
				}, {
					"name": "doubleField",
					"type": "double"
				}, {
					"name": "byteField",
					"type": "bytes"
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema, "non-existing-field")

		then:
		assert tableSchema != null
		assert tableSchema.fields.get(0) != null
		assert tableSchema.fields.get(0).name == 'intField'
		assert tableSchema.fields.get(0).mode == 'REQUIRED'
		assert tableSchema.fields.get(1) != null
		assert tableSchema.fields.get(1).name == 'longField'
		assert tableSchema.fields.get(2) != null
		assert tableSchema.fields.get(2).name == 'floatField'
		assert tableSchema.fields.get(2).type == 'FLOAT'
		assert tableSchema.fields.get(3) != null
		assert tableSchema.fields.get(3).name == 'doubleField'
		assert tableSchema.fields.get(3).type == 'FLOAT'
		assert tableSchema.fields.get(4) != null
		assert tableSchema.fields.get(4).name == 'byteField'
	}

	def "should skip entaire record and internal field"() {
		given:

		def avroSchema = new Schema.Parser().parse('''
			{ 	"name": "simpleSchema",
				"type": "record",
				"fields": [ {
					"name": "intField",
					"type": "int"
				}, {
					"name": "recordField",
					"type": [ {
						"type": "record",
						"name": "RecordField",
						"fields": [ {
							"name": "internalField",
							"type": "float"
							}, {
								"name": "internalNullableField",
								"type": ["bytes", "null"]
							}, {
								"name": "internalNullableRecord",
								"type": {
									"type": "record",
									"name": "InternalNullableRecord",
									"fields": [ {
										"name": "internalField1",
										"type": ["string", "null"]
									} ]
								}
							} ]
					}, "null"]
				}, {
					"name": "recordField2",
					"type": [ {
						"type": "record",
						"name": "RecordField2",
						"fields": [ {
							"name": "internalField",
							"type": "float"
							}, {
								"name": "internalNullableField",
								"type": ["bytes", "null"]
							} 
						]
					} ]
				} ]
			}
		''')

		when:
		def tableSchema = AvroSchemaConverter.toTableSchema(avroSchema, 'recordField2','internalNullableRecord')

		then:
		assert tableSchema != null
		assert tableSchema.fields != null

		def intField = tableSchema.fields.get(0)
		assert intField != null
		assert intField.name == 'intField'
		assert intField.type == 'INTEGER'

		def recordField = tableSchema.fields.get(1)
		assert recordField != null
		assert recordField.name == 'recordField'
		assert recordField.type == 'RECORD'
		assert recordField.mode == 'NULLABLE'
		assert recordField.fields != null
		assert recordField.fields.size() == 2

		def internalField = recordField.fields.get(0)
		assert internalField != null
		assert internalField.name == 'internalField'
		assert internalField.type == 'FLOAT'
		assert internalField.mode == 'REQUIRED'

		def internalNullableField = recordField.fields.get(1)
		assert internalNullableField != null
		assert internalNullableField.name == 'internalNullableField'
		assert internalNullableField.type == 'BYTES'
		assert internalNullableField.mode == 'NULLABLE'
	}
}
