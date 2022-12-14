package com.example.saa_project_db_engine.services.models

import android.util.Log
import com.example.saa_project_db_engine.db.base.LogicalPage
import com.example.saa_project_db_engine.db.base.PageData
import com.example.saa_project_db_engine.db.base.WithByteUtils
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import org.apache.avro.Schema

interface QueryTypeHandlerOnSelect {
    fun handle(page: HeapLogicalPage, index: Int, row: TableRow)
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
}

// pass update values here
class UpdateHandler() : QueryTypeHandlerOnSelect {
    // pass update specifics to the class in constructor
    override fun handle(page: HeapLogicalPage, index: Int, row: TableRow) {

    }

}

class DeleteHandler() : QueryTypeHandlerOnSelect {
    override fun handle(page: HeapLogicalPage, index: Int, row: TableRow) {
        page.delete(index, row)
    }
}