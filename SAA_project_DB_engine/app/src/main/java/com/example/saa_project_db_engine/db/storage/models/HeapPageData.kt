package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.db.base.PageData
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class HeapPageData(
    override var id: Int,
    override var previousPageId: Int,
    override var nextPageId: Int,
    override var records: MutableList<TableRow>,
    var rowOffsetArray: MutableList<RowOffsetArrayEntry>
) : PageData<TableRow>() {

    companion object {
        fun fromBytes(bytes: ByteBuffer): HeapPageData {
            val record =
                GenericRecord(SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName))
            record.load(bytes)
            val id = record.get("id") as Int
            val previousPageId = record.get("previousPageId") as Int
            val nextPageId = record.get("nextPageId") as Int
            val records = record.get("records") as MutableList<*>
            val mappedRecords = records.map {
                val indexedRecord = it as IndexedRecord
                val row = TableRow(indexedRecord.get(1) as ByteBuffer, indexedRecord.get(0) as Int)
                row
            }.toMutableList()
            val rowOffsetArray = record.get("offsetArray") as MutableList<*>
            val mappedRowOffsetArray = rowOffsetArray.map {
                val indexedRecord = it as IndexedRecord
                val row =
                    RowOffsetArrayEntry(indexedRecord.get(0) as Int, indexedRecord.get(1) as Int)
                row
            }.toMutableList()
            return HeapPageData(
                id,
                previousPageId,
                nextPageId,
                mappedRecords,
                mappedRowOffsetArray
            )
        }
    }

    override fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("id", id)
        record.put("previousPageId", previousPageId)
        record.put("nextPageId", nextPageId)
        record.put("records", records)
        record.put("offsetArray", rowOffsetArray)
        return record.toByteBuffer()
    }

}