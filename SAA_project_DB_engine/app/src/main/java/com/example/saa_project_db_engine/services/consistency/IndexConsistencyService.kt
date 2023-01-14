package com.example.saa_project_db_engine.services.consistency

import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import java.nio.ByteBuffer

object IndexConsistencyService {
    // key -> index field name (to access tree)
    // val -> the index record (as a serialized buffer, ready to be searched) and the row and page id
    // TODO: after every query, reset this map to a clean slate
    val affectedFieldsMap = mutableMapOf<String, MutableSet<Pair<ByteBuffer, IndexValues>>>()

    fun addAffectedFieldEntry(index: String, affectedRow: Pair<ByteBuffer, IndexValues>) {
        var shouldAdd = true
        val mapEntry = affectedFieldsMap.getOrPut(index) {
            shouldAdd = false
            mutableSetOf(affectedRow)
        }
        if (shouldAdd) {
            mapEntry.add(affectedRow)
        }
    }

    fun clear() {
        affectedFieldsMap.clear()
    }
}