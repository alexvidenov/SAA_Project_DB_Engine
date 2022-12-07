package com.example.saa_project_db_engine.db.managers.file

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexLogicalPage
import com.example.saa_project_db_engine.db.indexing.models.IndexPageData
import com.example.saa_project_db_engine.db.indexing.models.KeyValue
import com.example.saa_project_db_engine.db.indexing.models.nodes.NodeType
import com.example.saa_project_db_engine.db.managers.file.models.PageMetadata
import java.io.File

class IndexFileManager constructor(
    file: File,
    metadata: PageMetadata? = null
) : FileManager<KeyValue, IndexPageData, IndexLogicalPage>(file, metadata) {

    companion object {
        fun new(file: File): IndexFileManager {
            val fileManager = IndexFileManager(file, createPageMetadata())
            val rootPage =
                fileManager.allocateNewIndexLogicalPage(NodeType.LeafNode, mutableListOf())
            fileManager.writeModel(rootPage)
            return fileManager
        }

        fun load(file: File) = IndexFileManager(file)
    }

    fun allocateNewIndexLogicalPage(
        nodeType: NodeType,
        initialRecords: MutableList<KeyValue>
    ): IndexLogicalPage {
        val freePageId = nextLogicalPageId
        val freePage = readModel(freePageId)
        nextLogicalPageId = freePage?.nextId ?: (freePageId + 1)
        nextRowId += initialRecords.size
        writeMetadata()
        return IndexLogicalPage.new(freePageId, nodeType, initialRecords)
    }

    override fun readModel(pageId: Int): IndexLogicalPage? {
        val buffer = readBuffer(pageId)
        return if (buffer == null) {
            null
        } else {
            IndexLogicalPage.load(buffer)
        }
    }

    override fun writeModel(model: IndexLogicalPage) {
        writeBuffer(model.id, model.dump())
        Log.d("TEST", "NEXT ROW ID IN HEAP MANAGER IS :${nextRowId}")
        writeMetadata() // persists next system-maintained rowId
    }
}