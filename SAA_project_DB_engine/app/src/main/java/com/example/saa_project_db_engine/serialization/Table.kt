package com.example.saa_project_db_engine.serialization

import org.apache.avro.Schema
import java.nio.ByteBuffer

//
//class Table(schema: Schema) {
//    val value: GenericRecord.IO
//
//    init {
//        value = GenericRecord.IO(schema)
//    }
//
//    inner class Value : GenericRecord(value)
//    inner class Record(val value: Value)
//
//    fun createValue(byteBuffer: ByteBuffer): Value {
//        val record = Value()
//        record.load(byteBuffer)
//        return record
//    }
//
//    fun createRecord(valueByteBuffer: ByteBuffer): Record {
//        return Record(createValue(valueByteBuffer))
//    }
//}
