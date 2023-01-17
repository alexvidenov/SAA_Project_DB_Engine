package com.example.saa_project_db_engine.db.indexing.models.nodes

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.indexing.models.IndexLogicalPage
import com.example.saa_project_db_engine.toByteArray
import com.example.saa_project_db_engine.toByteBuffer
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.lang.Exception
import java.nio.ByteBuffer

open class InternalNode constructor(page: IndexLogicalPage, keyCompare: KeyCompare) :
    LeafNode(page, keyCompare) {
    companion object {
        private val encoder = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
        private val decoder = DecoderFactory.get().binaryDecoder(byteArrayOf(), null)

        fun encodeChildPageId(id: Int): ByteBuffer {
            val output = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(output, encoder)
            encoder.writeInt(id)
            encoder.flush()
            return output.toByteArray().toByteBuffer()
        }

        fun decodeChildPageId(byteBuffer: ByteBuffer): Int {
            val decoder = DecoderFactory.get().binaryDecoder(byteBuffer.toByteArray(), decoder)
            return decoder.readInt()
        }
    }

    fun findChildPageId(key: ByteBuffer): Int {
        return when (val result = find(key)) {
            is FindResult.ExactMatch -> decodeChildPageId(result.keyValue.value)
            is FindResult.FirstGreaterThanMatch -> {
                if (result.index == 0 && previousId != -1)
                    throw Exception()
                val index = if (result.index == 0) 0 else result.index - 1
                decodeChildPageId(page.records[index].value)
            }
            else -> -1
        }
    }

    fun firstChildPageId(): Int {
        return decodeChildPageId(records.first().value)
    }

    fun lastChildPageId(): Int {
        return decodeChildPageId(records.last().value)
    }

    fun addChildNode(node: Node, minKey: ByteBuffer = node.minRecord.key) {
        val childPageId = encodeChildPageId(node.id)
        put(minKey, childPageId)
    }
}