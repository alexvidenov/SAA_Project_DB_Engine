package com.example.saa_project_db_engine.db.indexing.models

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

data class KeyValue(val key: ByteBuffer, val value: ByteBuffer) : SchemaAware(),
    IndexedRecord,
    WithByteUtils {
    private val crc: UInt
        get() = CRC32().let {
            it.update(key.toByteArray().asUByteArray())
            it.update(value.toByteArray().asUByteArray())
            it.value
        }

    companion object {
        fun fromBytes(bytes: ByteBuffer): KeyValue {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val key = record.get("key") as ByteBuffer
            val value = record.get("value") as ByteBuffer
            val crc32 = CRC32()
            crc32.update(key.toByteArray().asUByteArray())
            crc32.update(value.toByteArray().asUByteArray())
            val crc = record.get("crc") as UInt
            if (crc32.value == crc) {
                return KeyValue(key, value)
            }
            throw CRC32CheckFailedException("")
        }
    }

    override fun getSchema(): Schema {
        return fileSchema
    }

    override fun put(i: Int, v: Any?) {
    }

    override fun get(i: Int): Any {
        return when (i) {
            0 -> this.key
            1 -> this.value
            2 -> this.crc
            else -> {}
        }
    }

    override fun toAvroBytesSize(): Int {
        return this.key.toAvroBytesSize() + this.value.toAvroBytesSize() + this.crc.toAvroBytesSize()
    }

    override fun empty(): WithByteUtils {
        return KeyValue(ByteBuffer.allocate(0), ByteBuffer.allocate(0))
    }
}