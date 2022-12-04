package com.example.saa_project_db_engine.serialization

import org.apache.avro.Schema
import java.nio.ByteBuffer

class Table(schema: Schema) {
    private val record = GenericRecord(schema)

    fun insertRow() {

    }

    fun createRecord() {
    }

    fun select() {}

}
