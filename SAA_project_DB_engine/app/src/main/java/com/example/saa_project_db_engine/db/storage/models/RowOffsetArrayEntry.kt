package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import com.example.saa_project_db_engine.toAvroBytesSize
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class RowOffsetArrayEntry(var rowId: Int, var index: Int) : SchemaAware(), IndexedRecord,
    WithByteUtils {

    companion object {
        fun fromBytes(bytes: ByteBuffer): RowOffsetArrayEntry {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val rowId = record.get("rowId") as Int
            val index = record.get("index") as Int
            return RowOffsetArrayEntry(rowId, index)
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("rowId", rowId)
        record.put("index", index)
        return record.toByteBuffer()
    }

    override fun getSchema(): Schema {
        return fileSchema
    }

    override fun put(i: Int, v: Any?) {
    }

    override fun get(i: Int): Any {
        return when (i) {
            0 -> this.rowId
            1 -> this.index
            else -> {}
        }
    }

    override fun toAvroBytesSize(): Int {
        return rowId.toAvroBytesSize() + index.toAvroBytesSize()
    }

    override fun empty(): WithByteUtils {
        return RowOffsetArrayEntry(-1, -1)
    }
}