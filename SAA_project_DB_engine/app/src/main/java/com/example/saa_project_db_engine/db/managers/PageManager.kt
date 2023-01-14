package com.example.saa_project_db_engine.db.managers

import com.example.saa_project_db_engine.MAX_PAGE_SIZE
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.NullPersistenceModelException
import com.example.saa_project_db_engine.db.base.PageData
import com.example.saa_project_db_engine.db.managers.file.FileManager
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.db.base.LogicalPage

abstract class PageManager<B : WithByteUtils, D : PageData<B>, P : LogicalPage<B, D>>
    (protected open val fileManager: FileManager<B, D, P>) {
    protected val pool = hashMapOf<Int, P>()

    fun get(id: Int): P? {
        return if (pool.contains(id)) {
            pool[id]
        } else {
            val page = fileManager.readModel(id) ?: throw NullPersistenceModelException("")
            pool[id] = page
            page
        }
    }

    fun split(page: P): P {
        fun findSplitPoint(it: Iterator<IndexedValue<B>>, accumulatedSize: Int = 0): Int? {
            return if (it.hasNext()) {
                val next = it.next()
                val newSize = accumulatedSize + next.value.toAvroBytesSize()
                if (page.overheadSize + newSize > MAX_PAGE_SIZE / 2) next.index + 1 else findSplitPoint(
                    it,
                    newSize
                )
            } else {
                null
            }
        }

        val splitPoint =
            findSplitPoint(page.records.withIndex().iterator()) ?: (page.records.size - 1)
        val movingIndexes = splitPoint until page.records.size
        val newPage = move(page, movingIndexes)
        val nextPage = get(page.nextId)
        connect(page, newPage)
        connect(newPage, nextPage)
        commit(page)
        commit(newPage)
        commit(nextPage)
        return newPage
    }

    fun move(page: P, range: IntRange): P {
        val records = range.map {
            val record = page.records[it]
            page.delete(it, record)
        }.toMutableList()
        return allocateNewLogicalPage(page, records)
    }

    // page param is to reuse old values (node type in case of index)
    abstract fun allocateNewLogicalPage(page: P, records: MutableList<B>): P

    fun commit(page: P?) {
        if (page != null) {
            fileManager.writeModel(page)
        }
    }

    fun getRootPage(): P {
        return get(ROOT_PAGE_ID) ?: throw Exception()
    }

    private fun connect(previous: P, next: P?) {
        previous.nextId = next?.id ?: -1
        next?.previousId = previous.id
    }
}