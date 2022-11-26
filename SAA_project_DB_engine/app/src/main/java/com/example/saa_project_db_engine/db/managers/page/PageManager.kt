package com.example.saa_project_db_engine.db.managers.page

import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.PageData
import com.example.saa_project_db_engine.db.managers.file.FileManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage

abstract class PageManager<T : PageData>(protected open val fileManager: FileManager<T>) {
    protected val pool = hashMapOf<Int, T>()

    fun get(id: Int): T? {
        return if (pool.contains(id)) {
            pool[id]
        } else {
            val page = fileManager.readModel(id) ?: throw java.lang.Exception()
            pool[id] = page
            page
        }
    }

//    fun split(page: Page): Page {
//        fun findSplitPoint(it: Iterator<IndexedValue<KeyValue>>, accumulatedSize: Int = 0): Int? {
//            return if (it.hasNext()) {
//                val next = it.next()
//                val newSize = accumulatedSize + next.value.toAvroBytesSize()
//                if (Page.sizeOverhead + newSize > MAX_PAGE_SIZE/2) next.index+1 else findSplitPoint(it, newSize)
//            } else {
//                null
//            }
//        }
//        val splitPoint = findSplitPoint(page.records.withIndex().iterator()) ?: page.records.size-1
//        val movingIndexes = splitPoint until page.records.size
//        val newPage = move(page, page.nodeType, movingIndexes)
//        val nextPage = get(page.nextId)
//        connect(page, newPage)
//        connect(newPage, nextPage)
//        commit(page)
//        commit(newPage)
//        commit(nextPage)
//        return newPage
//    }

//    fun move(page: Page, nodeType: NodeType, range: IntRange): Page {
//        val records = range.map { page.delete(range.first) }.toMutableList()
//        return allocate(nodeType, records)
//    }

    fun commit(page: T?) {
        if (page != null) {
            fileManager.writeModel(page)
        }
    }

    fun getRootPage(): T {
        return get(ROOT_PAGE_ID) ?: throw Exception()
    }

    private fun connect(previous: T?, next: T?) {
        previous?.nextId = next?.id
        next?.previousId = previous?.id
    }
}