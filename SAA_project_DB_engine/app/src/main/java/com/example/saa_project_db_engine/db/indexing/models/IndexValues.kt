package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class IndexValues(val records: MutableList<IndexValue>) : SchemaAware() {
    companion object {
        fun fromBytes(bytes: ByteBuffer): IndexValues {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val records = record.get("records") as MutableList<*>
            val mapped = records.map {
                val indexedRecord = it as IndexedRecord
                val row =
                    IndexValue(indexedRecord.get(0) as Int, indexedRecord.get(1) as Int)
                row
            }.toMutableList()
            return IndexValues(mapped)
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("records", records)
        return record.toByteBuffer()
    }
}