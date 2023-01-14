package com.example.saa_project_db_engine.db.managers.file.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import java.nio.ByteBuffer

data class PageMetadata(var nextLogicalPageId: Int, var nextRowId: Int, var entriesCount: Int) :
    SchemaAware() {
    companion object {
        fun fromBytes(bytes: ByteBuffer): PageMetadata {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val nextLogicalPageId = record.get("nextLogicalPageId") as Int
            val nextRowId = record.get("nextRowId") as Int
            val entriesCount = record.get("entriesCount") as Int
            return PageMetadata(
                nextLogicalPageId, nextRowId, entriesCount
            )
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("nextLogicalPageId", nextLogicalPageId)
        record.put("nextRowId", nextRowId)
        record.put("entriesCount", entriesCount)
        return record.toByteBuffer()
    }
}