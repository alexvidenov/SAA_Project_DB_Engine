package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import com.example.saa_project_db_engine.toAvroBytesSize
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class TableRow(var value: ByteBuffer, var rowId: Int? = -1) : SchemaAware(), IndexedRecord,
    WithByteUtils {

    companion object {
        fun fromBytes(bytes: ByteBuffer): TableRow {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val rowId = record.get("rowId") as Int
            val value = record.get("value") as ByteBuffer
            return TableRow(value, rowId)
        }
    }

    fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("rowId", rowId)
        record.put("value", value)
        return record.toByteBuffer()
    }

    override fun getSchema(): Schema {
        return fileSchema
    }

    override fun put(i: Int, v: Any?) {
    }

    override fun get(i: Int): Any {
        return when (i) {
            0 -> this.rowId!!
            1 -> this.value
            else -> {}
        }
    }

    override fun toAvroBytesSize(): Int {
        return rowId!!.toAvroBytesSize() + value.toAvroBytesSize()
    }

    override fun empty(): WithByteUtils {
        return TableRow(ByteBuffer.allocate(0), -1)
    }
}