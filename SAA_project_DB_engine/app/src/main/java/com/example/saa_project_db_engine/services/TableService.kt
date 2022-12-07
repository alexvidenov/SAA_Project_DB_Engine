package com.example.saa_project_db_engine.services

import android.content.Context
import android.util.Log
import androidx.constraintlayout.motion.widget.KeyTimeCycle
import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.indexing.models.BPlusTree
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.file.IndexFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.parsers.models.*
import com.example.saa_project_db_engine.serialization.GenericRecord
import org.apache.avro.Schema
import java.io.File

data class TableManagerData(
    val heapPageManager: HeapPageManager,
    val indexes: MutableList<BPlusTree>,
    val schema: Schema
)

class TableService constructor(ctx: Context) {
    private val dir = ctx.filesDir
    private var managerPool = hashMapOf<String, TableManagerData>()

    private val files: List<String>
        get() = dir.listFiles()?.filter { !it.isDirectory }?.map { it.name }!!

    // schema is Avro-compatible schema definition
    fun createTable(tableName: String, schema: String) {
        val schemaFile = File(dir, "$tableName.avsc")
        schemaFile.createNewFile()
        schemaFile.writeText(schema)

        val dbFile = File(dir, "$tableName.db")
        dbFile.createNewFile()

        val heapManager = HeapFileManager.new(dbFile)
        val pageManager = HeapPageManager(heapManager)

        managerPool[tableName] =
            TableManagerData(pageManager, mutableListOf(), Schema.Parser().parse(schemaFile))
    }

    fun createIndex(tableName: String, fieldName: String) {
        val indexFile = File(dir, "${tableName}_index_$fieldName.index")
        val schemaFile = File(dir, "${tableName}_index_${fieldName}_schema.avsc")
        val schema = Schema.Parser().parse(schemaFile)
        val type = schema.fields.find { it.name() == fieldName }?.schema()?.type
        indexFile.createNewFile()
        schemaFile.createNewFile()
        val builder = StringBuilder()
        val text = ""
        schemaFile.writeText(text)
    }

    private fun loadTable(tableName: String) {
        if (!managerPool.contains(tableName)) {
            val file = File(dir, "$tableName.db")
            val schemaFile = File(dir, "$tableName.avsc")
            val fileManager = HeapFileManager.load(file)
            val heapPageManager = HeapPageManager(fileManager)
            val indexManagers = initIndexManagersForTable(tableName)
            managerPool[tableName] =
                TableManagerData(heapPageManager, indexManagers, Schema.Parser().parse(schemaFile))
        }
    }

    // tableName.db
    // tableName_index_name.index
    // tableName_index_name_schema.avsc
    private fun initIndexManagersForTable(tableName: String): MutableList<BPlusTree> {
        val indexes = mutableListOf<IndexPageManager>()
        val compares = mutableListOf<KeyCompare>()
        val managers = mutableListOf<BPlusTree>()
        files.filter {
            it.startsWith("${tableName}_index")
        }.forEach {
            val file = File(dir, it)

            if (file.extension == "index") {
                val fileManager = IndexFileManager(File(dir, it))
                val indexPageManager = IndexPageManager(fileManager)
                indexes.add(indexPageManager)
            } else if (file.extension == "avsc") {
                val schema = Schema.Parser().parse(file)
                val record = GenericRecord(schema)
                compares.add(record.keyCompare)
            }
        }
        indexes.forEachIndexed { index, manager ->
            val compare = compares[index]
            managers.add(BPlusTree(manager, compare))
        }
        return managers
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
        inserts.removeAt(0) // first item it always empty due to way of parsing
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
                Log.d("TEST", "PUTTING DEFAULT: ${it.name()}, ${it.defaultVal()}")
                record.put(it.name(), it.defaultVal())
            }
            val buf = record.toByteBuffer()
            tableRows.add(TableRow(buf))
        }
        data.heapPageManager.insertRows(tableRows)
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
            Schema.Type.BOOLEAN -> {
                value.toBoolean()
            }
            else -> {}
        }
    }

    fun indexScan() {

    }

    fun scan() {

    }

    fun select(
        tableName: String,
        fields: List<String>,
        conditions: List<WhereClauseType.LogicalOperation>
    ): SelectResultModel {
        loadTable(tableName)
        val data = managerPool[tableName]!!

        var curPageId = ROOT_PAGE_ID

        val values: MutableList<MutableList<String>> = mutableListOf()

        loop@ while (true) {
            try {
                Log.d("TEST", "GETTING $curPageId")
                val page = data.heapPageManager.get(curPageId)
                Log.d("TEST", "records size: ${page!!.records.size}")

                page.records.forEach {
                    Log.d("TEST", "PAGE")
                    val record = GenericRecord(data.schema)

                    Log.d("TEST", "${it.value} ${it.rowId}")
                    record.load(it.value)

                    val res = parseSubExpression(record, conditions)

                    if (res) {
                        val array = mutableListOf<String>()
                        fields.forEach { f ->
                            Log.d("TEST", "FIELD: $f")
                            val field = record.get(f)
                            Log.d("TEST", "FIELD2: $field")
                            array.add(field.toString())
                        }
                        values.add(array)
                    }
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

    // (Id == '6' OR Date == '22'') AND Date >= '23
    private fun parseSubExpression(
        record: GenericRecord,
        ops: List<WhereClauseType.LogicalOperation>
    ): Boolean {
        var currentOpType: LogicalOperator?
        var currentCachedOperationResult = true
        var i = 0
        Log.d("TEST", "operations: $ops")
        while (i <= ops.size - 1) {
            val op = ops[i]
            Log.d("TEST", "operation: $op")
            currentOpType = op.operator
            val parseState = parseLogicalOperation(record, op)
            Log.d("TEST", "Empty state: ${parseState.empty}")
            when (parseState.empty) {
                LogicalOperationParseResultEmpty.NONE -> { // only the init value in the logical foldl (from FP)
                    i++
                    currentCachedOperationResult = parseState.result
                }
                LogicalOperationParseResultEmpty.LEFT -> {
                    i++
                    currentCachedOperationResult = applyOperation(
                        currentCachedOperationResult,
                        parseState.result,
                        currentOpType!!
                    )
                }
                LogicalOperationParseResultEmpty.RIGHT -> {
                    val next = ops[i + 1]
                    i += 2
                    val nextState = parseLogicalOperation(record, next)
                    currentCachedOperationResult = applyOperation(
                        currentCachedOperationResult,
                        nextState.result,
                        currentOpType!!
                    )
                }
            }
            Log.d("TEST", "currentCachedOperationResult: $currentCachedOperationResult")
        }
        return currentCachedOperationResult
    }

    private fun applyOperation(op1: Boolean?, op2: Boolean?, operator: LogicalOperator): Boolean {
        return when (operator) {
            LogicalOperator.AND -> op1!! && op2!!
            LogicalOperator.OR -> op1!! || op2!!
            LogicalOperator.NOT -> !op2!!
        }
    }

    private fun parseLogicalOperation(
        record: GenericRecord,
        op: WhereClauseType.LogicalOperation
    ): LogicalOperationParseResult {
        var left: Boolean? = null
        var right: Boolean? = null
        op.leftNode?.let {
            left = parseCondition(record, it)
            Log.d("TEST", "parsed left node: $left")
        }
        op.rightNode?.let {
            right = parseCondition(record, it)
            Log.d("TEST", "parsed right node: $right")
        }
        op.leftSubExpr?.let {
            left = parseSubExpression(record, it)
            Log.d("TEST", "parsed left subexpr: $left")
        }
        op.rightSubExpr?.let {
            right = parseSubExpression(record, it)
            Log.d("TEST", "parsed right subexpr: $right")
        }

        return if (left != null && right != null) {
            val res = applyOperation(left, right, op.operator!!)
            LogicalOperationParseResult(res, LogicalOperationParseResultEmpty.NONE)
        } else {
            if (left == null) {
                LogicalOperationParseResult(right!!, LogicalOperationParseResultEmpty.LEFT)
            } else {
                LogicalOperationParseResult(left!!, LogicalOperationParseResultEmpty.RIGHT)
            }
        }
    }

    private fun parseCondition(record: GenericRecord, cond: WhereClauseType.Condition): Boolean {
        var op1: Any? = null
        var op2: Any? = null
        var typeToConvert: Schema.Type? = null
        var res = false
        when (cond.operand1Type) {
            ConditionType.LITERAL -> {
                op1 = cond.operand1
            }
            ConditionType.FIELD -> {
                op1 = record.get(cond.operand1)
                if (op1 is org.apache.avro.util.Utf8) {
                    op1 = op1.toString()
                }
                typeToConvert = fieldType(record, cond.operand1)
            }
            else -> {}
        }
        when (cond.operand2Type) {
            ConditionType.LITERAL -> {
                op2 = cond.operand2
                op2 = convertStringToNativeType(op2, typeToConvert!!)
            }
            ConditionType.FIELD -> {
                op2 = record.get(cond.operand2)
            }
            else -> {}
        }
        Log.d(
            "TEST",
            "CONDITION: \n$op1\n$op2\n${cond.operator} ${op1!!.javaClass.name} ${op2!!.javaClass.name}"
        )
        op1 = (op1 as Comparable<Any>)
        op2 = (op2 as Comparable<*>)
        when (cond.operator) {
            Operator.Eq -> res = op1 == op2
            Operator.Ne -> res = op1 != op2
            Operator.Gt -> res = op1 > op2
            Operator.Lt -> res = op1 < op2
            Operator.Gte -> res = op1 >= op2
            Operator.Lte -> res = op1 <= op2
            else -> {}
        }
        return res
    }

    private fun fieldType(record: GenericRecord, field: String): Schema.Type {
        return record.schema.fields.find {
            it.name() == field
        }!!.schema().type
    }
}