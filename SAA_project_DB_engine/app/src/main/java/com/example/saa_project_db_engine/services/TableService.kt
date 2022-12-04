package com.example.saa_project_db_engine.services

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import org.apache.avro.Schema
import java.io.File

data class TableManagerData(val pageManager: HeapPageManager, val schema: Schema)

class TableService constructor(private val ctx: Context) {
    private val dir = ctx.filesDir
    private var managerPool = hashMapOf<String, TableManagerData>()

    // schema is Avro-compatible schema definition
    fun createTable(tableName: String, schema: String) {
        val schemaFile = File(dir, "$tableName.avsc")
        schemaFile.createNewFile()
        schemaFile.writeText(schema)

        val dbFile = File(dir, "$tableName.db")
        dbFile.createNewFile()

        val heapManager = HeapFileManager.new(dbFile)
        val pageManager = HeapPageManager(heapManager)

        managerPool[tableName] = TableManagerData(pageManager, Schema.Parser().parse(schemaFile))
    }

    private fun loadTable(tableName: String) {
        if (!managerPool.contains(tableName)) {
            val file = File(dir, "$tableName.db")
            val schemaFile = File(dir, "$tableName.avsc")
            val fileManager = HeapFileManager.load(file)
            val pageManager = HeapPageManager(fileManager)
            managerPool[tableName] =
                TableManagerData(pageManager, Schema.Parser().parse(schemaFile))
        }
    }

    fun insertRows(
        tableName: String, fields: MutableList<String>,
        inserts: MutableList<MutableList<String>>
    ) {
        loadTable(tableName)
        val data = managerPool[tableName]!!
        val tableRows = mutableListOf<TableRow>()
        val schema = data.schema
        val omittedFields = schema.fields.filter {
            !fields.contains(it.name())
        }
        inserts.removeAt(0)
        inserts.forEach {
            val record = GenericRecord(data.schema)
            fields.forEachIndexed { index, s ->
                val field = schema.fields.find {
                    it.name() == s
                }
                val value = convertStringToNativeType(it[index], field!!.schema().type)
                Log.d("TEST", "PUTTING: $s, $value")
                record.put(s, value)
            }
            omittedFields.forEach {
                Log.d("TEST", "PUTTING: ${it.name()}, ${it.defaultVal()}")
                record.put(it.name(), it.defaultVal())
            }
            val buf = record.toByteBuffer()
            tableRows.add(TableRow(buf))
        }
        data.pageManager.insertRows(tableRows)
    }

    private fun convertStringToNativeType(value: String, type: Schema.Type): Any {
        return when (type) {
            Schema.Type.STRING -> {
                value
            }
            Schema.Type.INT -> {
                value.toInt()
            }
            Schema.Type.LONG -> {
                value.toLong()
            }
            Schema.Type.FLOAT -> {
                value.toFloat()
            }
            Schema.Type.DOUBLE -> {
                value.toDouble()
            }
            Schema.Type.BOOLEAN -> TODO()
            else -> {}
        }
    }

    fun indexScan() {

    }

    fun scan() {

    }

    fun select(tableName: String, fields: List<String>): SelectResultModel {
        loadTable(tableName)
        val data = managerPool[tableName]!!

        var curPageId = ROOT_PAGE_ID

        val values: MutableList<MutableList<String>> = mutableListOf()

        loop@ while (true) {
            try {
                Log.d("TEST", "GETTING $curPageId")
                val page = data.pageManager.get(curPageId)
                Log.d("TEST", "records size: ${page!!.records.size}")

                page.records.forEach {
                    Log.d("TEST", "PAGE")
                    val record = GenericRecord(data.schema)

                    Log.d("TEST", "${it.value} ${it.rowId}")
                    record.load(it.value)

                    val array = mutableListOf<String>()
                    fields.forEach { f ->
                        Log.d("TEST", "FIELD: $f")
                        val field = record.get(f)
                        Log.d("TEST", "FIELD2: $field")
                        array.add(field.toString())
                    }
                    values.add(array)
                }

                curPageId++
            } catch (e: java.lang.Exception) {
                Log.d("TEST", "BREAKING LOOP LABEL")
                break@loop
            }
        }

        val model = SelectResultModel(fields, values)
        Log.d("TEST", "SELECT RESULT MODEL: $model")

        return model
    }

    fun test() {
        val tableName = "Sample"
        loadTable(tableName)
        val data = managerPool[tableName]!!
        val page = data.pageManager.get(ROOT_PAGE_ID)

        val record = GenericRecord(data.schema)

        page!!.records.forEach {
            record.load(it.value)
            val id = record.get("Id")
            val pass = record.get("Name")
            Log.d("TEST", "Id: $id")
            Log.d("TEST", "Name: $pass")
        }
    }
}