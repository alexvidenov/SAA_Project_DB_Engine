package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import java.nio.ByteBuffer

data class IndexValue(var pageId: Int, var rowId: Int) : SchemaAware() {
    companion object {
        fun fromBytes(bytes: ByteBuffer): IndexValue {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val pageId = record.get("pageId") as Int
            val rowId = record.get("rowId") as Int
            return IndexValue(
                pageId, rowId
            )
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("pageId", pageId)
        record.put("rowId", rowId)
        return record.toByteBuffer()
    }
}