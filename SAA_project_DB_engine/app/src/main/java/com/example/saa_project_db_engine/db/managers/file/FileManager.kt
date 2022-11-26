package com.example.saa_project_db_engine.db.managers.file

import com.example.saa_project_db_engine.*
import com.example.saa_project_db_engine.avro.PageMetadata
import com.example.saa_project_db_engine.db.PageData
import com.example.saa_project_db_engine.db.managers.file.extensions.getValue
import com.example.saa_project_db_engine.db.managers.file.extensions.setValue
import java.io.EOFException
import java.io.RandomAccessFile
import java.io.File
import java.lang.Exception
import java.nio.ByteBuffer

abstract class FileManager<T : PageData> protected constructor(
    file: File,
    initialMetadata: PageMetadata? = null
) {

    companion object {
        @JvmStatic
        protected fun createPageMetadata(): PageMetadata {
            val builder = PageMetadata.newBuilder()
            builder.nextLogicalPageId = ROOT_PAGE_ID
            builder.nextRowId = 0
            return builder.build()
        }
    }

    private val file = RandomAccessFile(file, "rws")
    private val metadata = initialMetadata ?: loadMetadata()
    private val buffer = ByteArray(MAX_PAGE_SIZE)

    var nextLogicalPageId: Int? by metadata
    var nextRowId: Int? by metadata

    abstract fun readModel(pageId: Int): T?

    abstract fun writeModel(model: T)

    protected fun readBuffer(id: Int): ByteBuffer? {
        return try {
            seekPage(id)
            file.readFully(buffer)
            buffer.toByteBuffer()
        } catch (e: EOFException) {
            null
        }
    }

    protected fun writeBuffer(id: Int, buf: ByteBuffer) {
        seekPage(id)
        buf.toByteArray(buffer)
        file.write(buffer)
    }

    private fun loadMetadata(): PageMetadata {
        val metadataBuffer = ByteArray(METADATA_SIZE)
        file.seek(0)
        file.readFully(metadataBuffer)
        return PageMetadata.fromByteBuffer(metadataBuffer.toByteBuffer())
    }

    protected fun writeMetadata() {
        file.seek(0)
        val byteBuffer = metadata.toByteBuffer()
        if (byteBuffer.limit() > METADATA_SIZE) throw Exception()
        file.write(byteBuffer.toByteArray())
    }

    private fun seekPage(id: Int) {
        val pos = id * MAX_PAGE_SIZE + METADATA_SIZE
        file.seek(pos.toLong())
    }
}