package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.KeyCompare
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import java.io.File

open class LeafNode constructor(page: IndexLogicalPage, keyCompare: KeyCompare) :
    Node(page, keyCompare) {
}