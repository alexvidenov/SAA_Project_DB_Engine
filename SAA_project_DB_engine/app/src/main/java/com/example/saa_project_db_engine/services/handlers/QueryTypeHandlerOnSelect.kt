package com.example.saa_project_db_engine.services.handlers

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import com.example.saa_project_db_engine.services.models.IndexData
import org.apache.avro.Schema
import java.nio.ByteBuffer

interface QueryTypeHandlerOnSelect {
    fun handle(manager: HeapPageManager, page: HeapLogicalPage, index: Int, row: TableRow)
    fun cleanup()
}

data class QueryTypeHandler(
    val handler: QueryTypeHandlerOnSelect,
    val persistCbk: (cbk: (() -> Unit)?) -> Unit
)

data class SelectOpts(val orderByField: String, val distinct: Boolean)

typealias SelectResArray = MutableList<Pair<Comparable<Any>, MutableList<String>>>

/* DISTINCT flow:
    on every table row fetched,
    add to all the sets (n sets, n determined from the fields)
    on cleanup, check the set with the most values
    traverse all records again (which means we need them beforehand as well)
    and add to a hashmap with the key the value from the set with the most values.
    The value will be all the remaining fields from the select
    Update the UI field with the entries.forEach.

    DISTINCT -> ORDER BY
*/

class SelectHandler constructor(
    private val fields: List<String>,
    private val opts: SelectOpts,
    private val schema: Schema
) :
    QueryTypeHandlerOnSelect {
    val res: SelectResultModel
        get() = SelectResultModel(fields, uiModel)

    private var uiModel: MutableList<MutableList<String>> = mutableListOf()

    private val sortModel: SelectResArray = mutableListOf()

    private val distinctSetMap: MutableMap<String, MutableSet<Comparable<Any>>> = mutableMapOf()

    private val distinctRes: MutableMap<Comparable<Any>, MutableList<Comparable<Any>>> =
        mutableMapOf()

    private val rows: MutableList<GenericRecord> = mutableListOf()

    init {
        fields.forEach {
            distinctSetMap[it] = mutableSetOf()
        }
    }

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        val array = mutableListOf<String>()
        val record = GenericRecord(schema)
        record.load(row.value)
        rows.add(record)
        array.add(row.rowId.toString())
        fields.forEach { f ->
            Log.d("TEST", "FIELD: $f")
            val field = record.get(f)
            if (opts.distinct) {
                distinctSetMap[f]?.add(field as Comparable<Any>) // trust me, bro
            }
            Log.d("TEST", "FIELD2: $field")
            array.add(field.toString())
        }
        if (opts.orderByField != "") {
            var fieldValue = record.get(opts.orderByField)
            if (fieldValue is org.apache.avro.util.Utf8) {
                fieldValue = fieldValue.toString()
            }
            sortModel.add(Pair(fieldValue!! as Comparable<Any>, array)) // trust me, bro
        } else {
            uiModel.add(array)
        }
    }

    override fun cleanup() {
        if (sortModel.isNotEmpty()) {
            com.example.saa_project_db_engine.algos.quickSort(sortModel, 0, sortModel.size - 1)
            uiModel = sortModel.map {
                it.second
            }.toMutableList()
        }
    }

    private fun distinctHandler() {
        var max = 0
        var fieldWithMostDistinct = ""
        distinctSetMap.entries.forEach {
            val size = it.value.size
            if (size > max) {
                max = size
            }
            fieldWithMostDistinct = it.key
        }
        rows.forEach { record ->
            val value = record.get(fieldWithMostDistinct)
            val list = mutableListOf<Comparable<Any>>()
            fields.forEach {
                list.add(record.get(it) as Comparable<Any>)
            }
            distinctRes[value as Comparable<Any>] = list
        }

    }

    private fun orderByHandler(sortModel: SelectResArray) {
        com.example.saa_project_db_engine.algos.quickSort(sortModel, 0, sortModel.size - 1)
        uiModel = sortModel.map {
            it.second
        }.toMutableList()
    }
}

// pass update values here
class UpdateHandler() : QueryTypeHandlerOnSelect {
    // pass update specifics to the class in constructor
    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        val newVal = row // TODO: update new fields, create new Generic record
        page.update(index, newVal)
    }

    override fun cleanup() {
        // update index
    }

}

class DeleteHandler constructor(private val indexes: Map<String, IndexData>) :
    QueryTypeHandlerOnSelect {

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        Log.d("DELETE", "deleting row: ${row.rowId}")
        page.delete(index, row)
        manager.recordsCount--
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