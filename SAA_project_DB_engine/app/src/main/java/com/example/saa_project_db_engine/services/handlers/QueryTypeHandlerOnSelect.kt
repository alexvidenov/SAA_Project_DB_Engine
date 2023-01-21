package com.example.saa_project_db_engine.services.handlers

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
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

data class SelectOpts(val orderByFields: List<String>, val distinct: Boolean)

typealias SelectResArray = MutableList<Pair<List<Comparable<Any>>, MutableList<String>>>

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
        get() = SelectResultModel(uiTableHeaderModel, uiModel)

    private var uiTableHeaderModel = mutableListOf<String>()

    private var uiModel: MutableList<MutableList<String>> = mutableListOf()

    private val sortModel: SelectResArray = mutableListOf()

    private val distinctSetMap: MutableMap<String, MutableSet<Comparable<Any>>> = mutableMapOf()

    // extract type aliases
    private val distinctRes: MutableMap<Pair<String, Comparable<Any>>, MutableList<Pair<String, Comparable<Any>>>> =
        mutableMapOf()

    private val rows: MutableList<GenericRecord> = mutableListOf()

    init {
        Log.d("TEST", "OPTS ARE: $opts")
        if (opts.distinct) {
            fields.forEach {
                distinctSetMap[it] = mutableSetOf()
            }
        } else {
            uiTableHeaderModel.add("RowId")
        }
        uiTableHeaderModel.addAll(fields)
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
        if (opts.orderByFields.isNotEmpty() && !opts.distinct) {
            val comparables = mutableListOf<Comparable<Any>>()
            opts.orderByFields.forEach {
                var fieldValue = record.get(it)
                if (fieldValue is org.apache.avro.util.Utf8) {
                    fieldValue = fieldValue.toString()
                }
                comparables.add(fieldValue as Comparable<Any>)  // trust me, bro
            }
            sortModel.add(Pair(comparables, array))
        } else {
            Log.d("TEST", "ADDING TO UIMODEL")
            uiModel.add(array)
        }
    }

    override fun cleanup() {
        if (distinctSetMap.isNotEmpty() && opts.orderByFields.isNotEmpty()) {
            Log.d(
                "TEST",
                "distinctSetMap.isNotEmpty() && sortModel.isNotEmpty(): ${distinctSetMap} ${sortModel}"
            )
            val sortModel: SelectResArray = mutableListOf()
            distinctSetMapToDistinctRes()
            distinctRes.forEach { entry ->
                val array = mutableListOf<String>()
                val comparables = mutableListOf<Comparable<Any>>()
                opts.orderByFields.forEach { field ->
                    var fieldValue = if (entry.key.first == field) {
                        entry.key.second
                    } else {
                        entry.value.first {
                            it.first == field
                        }.second
                    }
                    if (fieldValue is org.apache.avro.util.Utf8) {
                        fieldValue = fieldValue.toString() as Comparable<Any>
                    }
                    comparables.add(fieldValue)
                }
                entry.value.forEach {
                    array.add(it.second.toString())
                }
                sortModel.add(Pair(comparables, array)) // trust me, bro
            }
            orderByHandler(sortModel)
        } else if (sortModel.isNotEmpty()) {
            Log.d("TEST", "sortModel.isNotEmpty(): $sortModel")
            orderByHandler(sortModel)
        } else if (distinctSetMap.isNotEmpty()) {
            distinctSetMapToDistinctRes()
            Log.d("TEST", "distinctSetMap.isNotEmpty(): $distinctRes")

            uiModel = distinctRes.map {
                val array = mutableListOf<String>()
                array.addAll(it.value.map {
                    it.second.toString()
                })
                array
            }.toMutableList()
        }
    }

    private fun orderByHandler(sortModel: SelectResArray) {
        val sorted = com.example.saa_project_db_engine.algos.quickSort(sortModel)
        uiModel = sorted.map {
            it.second
        }.toMutableList()
    }

    private fun distinctSetMapToDistinctRes() {
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
            val value = record.get(fieldWithMostDistinct) as Comparable<Any>
            val list = mutableListOf<Pair<String, Comparable<Any>>>()
            fields.forEach {
                list.add(Pair(it, record.get(it) as Comparable<Any>))
            }
            distinctRes[Pair(fieldWithMostDistinct, value)] = list
        }
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

class DeleteHandler :
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