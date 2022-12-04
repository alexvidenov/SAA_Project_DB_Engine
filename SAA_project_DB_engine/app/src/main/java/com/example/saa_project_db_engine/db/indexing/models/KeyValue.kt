package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.db.base.SchemaAware
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import com.example.saa_project_db_engine.toAvroBytesSize
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class KeyValue(val key: ByteBuffer, val value: ByteBuffer) : SchemaAware(), IndexedRecord,
    WithByteUtils {

    companion object {
        fun fromBytes(bytes: ByteBuffer): KeyValue {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val key = record.get("key") as ByteBuffer
            val value = record.get("value") as ByteBuffer
            return KeyValue(key, value)
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
            else -> {}
        }
    }

    override fun toAvroBytesSize(): Int {
        return this.key.toAvroBytesSize() + this.value.toAvroBytesSize()
    }

    override fun empty(): WithByteUtils {
        return KeyValue(ByteBuffer.allocate(0), ByteBuffer.allocate(0))
    }
}