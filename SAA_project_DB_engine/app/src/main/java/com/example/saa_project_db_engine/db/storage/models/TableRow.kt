package com.example.saa_project_db_engine.db.storage.models

import android.util.Log
import com.example.saa_project_db_engine.models.SchemaAware
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class TableRow(var value: ByteBuffer, var rowId: Int? = -1) : SchemaAware(), IndexedRecord {

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
        if (i == 0) {
            return this.rowId!!
        } else if (i == 1) {
            return this.value
        }
        return this
    }
}