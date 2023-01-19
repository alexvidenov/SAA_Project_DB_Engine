package com.example.saa_project_db_engine.services.consistency


import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.db.storage.models.TableRow
import java.nio.ByteBuffer

data class RowsToInsert(var tableName: String = "", var rows: MutableList<TableRow> = mutableListOf())

object IndexConsistencyService {
    // key -> index field name (to access tree)
    // val -> the index record (as a serialized buffer, ready to be searched) and the row and page id
    val affectedFieldsMap = mutableMapOf<String, MutableSet<Pair<ByteBuffer, IndexValues>>>()

    var rowsToInsert = RowsToInsert()

    // TODO: eventually, fix the fact that this updates only certain indexes. If there's more indexes ot a table that point to
    // certain row, it should be deleted as well. But since we won't have the key to search for.. you guessed it, full index search
    // with the value (page and row id)
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

    fun addInsertedRow(tableName: String, rows: List<TableRow>) {
        rowsToInsert.tableName = tableName
        rowsToInsert.rows.addAll(rows)
    }

    fun clear() {
        affectedFieldsMap.clear()
        rowsToInsert = RowsToInsert()
    }
}