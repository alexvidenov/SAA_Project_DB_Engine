package com.example.saa_project_db_engine.serialization

import com.example.saa_project_db_engine.toByteArray
import com.example.saa_project_db_engine.toByteBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

open class GenericRecord(sch: Schema) : GenericData.Record(sch) {
    private var io: IO = IO(sch)

    fun load(byteBuffer: ByteBuffer) {
        io.decode(this, byteBuffer)
    }

    fun toByteBuffer(): ByteBuffer {
        return io.encode(this)
    }

    class IO(private val schema: Schema) {
        init {
            if (schema.type != Schema.Type.RECORD) {
                throw IllegalArgumentException("Schema type must be record: $schema")
            }
        }

        private val writer = GenericDatumWriter<org.apache.avro.generic.GenericRecord>(schema)
        private val reader = GenericDatumReader<org.apache.avro.generic.GenericRecord>(schema)

        private var binaryEncoder: BinaryEncoder? = null
        private var binaryDecoder: BinaryDecoder? = null

        fun encode(record: GenericRecord): ByteBuffer {
            val output = ByteArrayOutputStream()
            binaryEncoder = EncoderFactory.get().binaryEncoder(output, binaryEncoder)
            writer.write(record, binaryEncoder)
            binaryEncoder?.flush()
            return output.toByteArray().toByteBuffer()
        }

        fun decode(record: GenericRecord, byteBuffer: ByteBuffer) {
            binaryDecoder =
                DecoderFactory.get().binaryDecoder(byteBuffer.toByteArray(), binaryDecoder)
            reader.read(record, binaryDecoder)
        }

        fun compare(a: ByteArray, b: ByteArray): Int {
            return BinaryData.compare(a, 0, b, 0, schema)
        }

        fun compare(a: ByteBuffer, b: ByteBuffer): Int {
            return compare(a.toByteArray(), b.toByteArray())
        }

        fun compare(a: GenericRecord, b: GenericRecord): Int {
            return compare(a.toByteBuffer(), b.toByteBuffer())
        }
    }
}