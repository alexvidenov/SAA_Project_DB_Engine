package com.example.saa_project_db_engine.db.managers.file

import com.example.saa_project_db_engine.avro.PageMetadata
import com.example.saa_project_db_engine.avro.TableRow
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import java.io.File
import java.lang.Exception
import java.nio.ByteBuffer

class HeapFileManager private constructor(
    file: File,
    metadata: PageMetadata? = null
) : FileManager<HeapLogicalPage>(file, metadata) {

    companion object {
        fun new(file: File): HeapFileManager {
            val fileManager = HeapFileManager(file, createPageMetadata())
            val rootPage = fileManager.allocateNewHeapLogicalPage(mutableListOf())
            fileManager.writeModel(rootPage)
            return fileManager
        }

        fun load(file: File) = HeapFileManager(file)
    }

    fun allocateNewHeapLogicalPage(
        initialRecords: MutableList<TableRow>
    ): HeapLogicalPage {
        val freePageId = nextLogicalPageId ?: throw Exception()
        val freePage = readModel(freePageId)
        nextLogicalPageId = if (freePage == null) {
            freePageId + 1
        } else {
            freePage.nextId ?: throw Exception()
        }
        if (nextRowId != null) {
            nextRowId = nextRowId!! + initialRecords.size
        }
        writeMetadata()
        return HeapLogicalPage.new(freePageId, initialRecords)
    }

    override fun readModel(pageId: Int): HeapLogicalPage? {
        val buffer = readBuffer(pageId)
        return if (buffer == null) {
            null
        } else {
            HeapLogicalPage.load(buffer)
        }
    }

    override fun writeModel(model: HeapLogicalPage) {
        writeBuffer(model.id, model.dump())
    }
}