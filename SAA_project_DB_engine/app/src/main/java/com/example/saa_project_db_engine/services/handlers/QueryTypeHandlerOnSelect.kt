package com.example.saa_project_db_engine.services.handlers

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import com.example.saa_project_db_engine.services.extensions.RowWithPageId
import com.example.saa_project_db_engine.services.extensions.convertOperandToNativeType
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

    // extract type aliases
    private val distinctRes: MutableMap<Pair<String, Comparable<Any>>, MutableList<Pair<String, Comparable<Any>>>> =
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
        if (opts.orderByField != "" && !opts.distinct) {
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
        if (distinctSetMap.isNotEmpty() && sortModel.isNotEmpty()) {
            val sortModel: SelectResArray = mutableListOf()
            distinctRes.forEach {
                var fieldValue: Any
                val array = mutableListOf<String>()
                array.add(it.key.second.toString())
                fieldValue = if (it.key.first == opts.orderByField) {
                    it.key.second
                } else {
                    it.value.first { it.first == opts.orderByField }.second
                }
                if (fieldValue is org.apache.avro.util.Utf8) {
                    fieldValue = fieldValue.toString()
                }
                it.value.forEach {
                    array.add(it.second.toString())
                }
                sortModel.add(Pair(fieldValue!! as Comparable<Any>, array)) // trust me, bro
            }
            orderByHandler(sortModel)
        } else if (sortModel.isNotEmpty()) {
            orderByHandler(sortModel)
        } else if (distinctSetMap.isNotEmpty()) {
            uiModel = distinctRes.map {
                val array = mutableListOf<String>()
                array.add(it.key.second.toString())
                array.addAll(it.value.map {
                    it.second.toString()
                })
                array
            }.toMutableList()
        }
    }

    private fun orderByHandler(sortModel: SelectResArray) {
        com.example.saa_project_db_engine.algos.quickSort(sortModel, 0, sortModel.size - 1)
        uiModel = sortModel.map {
            it.second
        }.toMutableList()
    }
}

data class OldNewPair(val old: RowWithPageId, val new: RowWithPageId)

class UpdateHandler(private val schema: Schema, private val updates: Map<String, String>) :
    QueryTypeHandlerOnSelect {
    val oldNewPairList = mutableListOf<OldNewPair>()

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        val record =
            GenericRecord(schema)
        record.load(row.value)
        updates.forEach {
            record.put(it.key, convertOperandToNativeType(it.value, it.key, record))
        }
        val newRow = TableRow(record.toByteBuffer(), row.rowId)
        oldNewPairList.add(OldNewPair(RowWithPageId(row, page.id), RowWithPageId(newRow, page.id)))
        page.update(index, newRow) // TODO: handle overflow
    }

    override fun cleanup() {
    }

}

class DeleteHandler constructor() :
    QueryTypeHandlerOnSelect {
    val rows = mutableListOf<RowWithPageId>()

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        Log.d("DELETE", "deleting row: ${row.rowId}")
        rows.add(RowWithPageId(row, page.id))
        page.delete(index, row)
        manager.recordsCount--
    }

    override fun cleanup() {
    }

}