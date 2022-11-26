package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.models.SchemaAware
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class HeapPageData(
    val id: Int,
    val previousPageId: Int,
    val nextPageId: Int,
    val records: MutableList<TableRow>,
) : SchemaAware() {

    companion object {
        fun fromBytes(bytes: ByteBuffer): HeapPageData {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val id = record.get("id") as Int
            val previousPageId = record.get("previousPageId") as Int
            val nextPageId = record.get("nextPageId") as Int
            val records = record.get("records") as MutableList<*> // IndexedRecords
            val mapped = records.map {
                val indexedRecord = it as IndexedRecord
                val row = TableRow(indexedRecord.get(0) as Int, indexedRecord.get(1) as ByteBuffer)
                row
            }.toMutableList()
            return HeapPageData(
                id,
                previousPageId,
                nextPageId,
                mapped
            )
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("id", id)
        record.put("previousPageId", previousPageId)
        record.put("nextPageId", nextPageId)
        record.put("records", records)
        return record.toByteBuffer()
    }

}