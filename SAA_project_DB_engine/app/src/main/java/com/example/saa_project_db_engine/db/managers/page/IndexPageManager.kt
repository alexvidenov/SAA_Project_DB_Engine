package com.example.saa_project_db_engine.db.managers.page

import com.example.saa_project_db_engine.db.indexing.models.IndexLogicalPage
import com.example.saa_project_db_engine.db.indexing.models.IndexPageData
import com.example.saa_project_db_engine.db.indexing.models.KeyValue
import com.example.saa_project_db_engine.db.managers.PageManager
import com.example.saa_project_db_engine.db.managers.file.IndexFileManager

class IndexPageManager constructor(override val fileManager: IndexFileManager) :
    PageManager<KeyValue, IndexPageData, IndexLogicalPage>(fileManager) {

    override fun allocateNewLogicalPage(
        page: IndexLogicalPage,
        records: MutableList<KeyValue>
    ): IndexLogicalPage {
        val allocated = fileManager.allocateNewIndexLogicalPage(page.data.nodeType, records)
        pool[allocated.id] = allocated
        return allocated
    }

}
