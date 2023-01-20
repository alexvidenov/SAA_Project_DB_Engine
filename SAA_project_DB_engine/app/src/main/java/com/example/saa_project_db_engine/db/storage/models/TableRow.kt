package com.example.saa_project_db_engine.db.storage.models

import android.util.Log
import com.example.saa_project_db_engine.algos.CRC32
import com.example.saa_project_db_engine.db.CRC32CheckFailedException
import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import com.example.saa_project_db_engine.toAvroBytesSize
import com.example.saa_project_db_engine.toByteArray
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class TableRow(var value: ByteBuffer, var rowId: Int? = -1) : SchemaAware(), IndexedRecord,
    WithByteUtils {
    val crc: UInt
        get() = CRC32().let {
            it.update(value.toByteArray().asUByteArray())
            it.value
        }

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
        val crc = CRC32().update(value.toByteArray().asUByteArray())
        record.put("crc", crc)
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
            2 -> this.crc.toInt()
            else -> {}
        }
    }

    override fun toAvroBytesSize(): Int {
        return rowId!!.toAvroBytesSize() + value.toAvroBytesSize() + crc.toAvroBytesSize()
    }

    override fun empty(): WithByteUtils {
        return TableRow(ByteBuffer.allocate(0), -1)
    }
}