package com.example.saa_project_db_engine.db.indexing.models

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import java.io.File

class LeafNode {

    fun test() {
        val schema = Schema.Parser().parse(File(""))
        val obj = GenericData.Record(schema)
        obj.get("name")
    }
}