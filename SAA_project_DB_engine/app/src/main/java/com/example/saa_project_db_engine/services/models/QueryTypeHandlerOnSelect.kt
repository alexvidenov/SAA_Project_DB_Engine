package com.example.saa_project_db_engine.services.models

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import org.apache.avro.Schema
import java.nio.ByteBuffer

interface QueryTypeHandlerOnSelect {
    fun handle(page: HeapLogicalPage, index: Int, row: TableRow)
    fun cleanup()
}

data class QueryTypeHandler(
    val handler: QueryTypeHandlerOnSelect,
    val persistCbk: (cbk: (() -> Unit)?) -> Unit
)

class SelectHandler constructor(private val fields: List<String>, private val schema: Schema) :
    QueryTypeHandlerOnSelect {
    val res: SelectResultModel
        get() = SelectResultModel(fields, returnModel)

    private val returnModel: MutableList<MutableList<String>> = mutableListOf()

    override fun handle(page: HeapLogicalPage, index: Int, row: TableRow) {
        val array = mutableListOf<String>()
        val record = GenericRecord(schema)
        record.load(row.value)
        array.add(row.rowId.toString())
        fields.forEach { f ->
            Log.d("TEST", "FIELD: $f")
            val field = record.get(f)
            Log.d("TEST", "FIELD2: $field")
            array.add(field.toString())
        }
        returnModel.add(array)
    }

    override fun cleanup() {
    }
}

// pass update values here
class UpdateHandler() : QueryTypeHandlerOnSelect {
    // pass update specifics to the class in constructor
    override fun handle(page: HeapLogicalPage, index: Int, row: TableRow) {
        val newVal = row // TODO: update new fields, create new Generic record
        page.update(index, newVal)
    }

    override fun cleanup() {
        // update index
    }

}

enum class RowAffectedOperation {
    UPDATE, DELETE
}

data class PageAndRowId(val pageId: Int, val rowId: Int)

class DeleteHandler constructor(private val indexes: Map<String, IndexData>) :
    QueryTypeHandlerOnSelect {

    override fun handle(page: HeapLogicalPage, index: Int, row: TableRow) {
        Log.d("DELETE", "deleting row: ${row.rowId}")
        page.delete(index, row)
    }

    override fun cleanup() {
        IndexConsistencyService.affectedFieldsMap.entries.forEach {
            val key = it.key
            val value = it.value
            val index = indexes[key]
            if (index != null) {
                val tree = index.tree
                value.forEach { pair ->
                    val record =
                        GenericRecord(index.indexSchema)
                    record.load(pair.first)
                    pair.second.records.forEach { idxVal ->
                        Log.d("TEST", "KEY TO DELETE: ${record}")
                        Log.d("TEST", "IDXVAL: ${idxVal}")
                        tree.delete(
                            IndexRecord(pair.first, ByteBuffer.allocate(0)),
                            idxVal.pageId,
                            idxVal.rowId
                        ) // first is the value of the index (Generic record with Ivan)
                    }
                }
            }
        }
    }

}