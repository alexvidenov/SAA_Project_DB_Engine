package com.example.saa_project_db_engine.services

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.MergeRule
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.indexing.models.*
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.file.IndexFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.db.managers.page.forEachRowPageIndexed
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.parsers.models.*
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.models.IndexData
import com.example.saa_project_db_engine.services.models.TableManagerData
import org.apache.avro.Schema
import java.io.File
import java.nio.ByteBuffer


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
            TableManagerData(pageManager, Schema.Parser().parse(schemaFile), mapOf())
    }

    fun createIndex(tableName: String, indexName: String, fieldName: String) {
        load(tableName)
        val data = managerPool[tableName]!!

        val indexFile = File(dir, "${tableName}_index_${indexName}_$fieldName.index")
        val fieldSchemaFile = File(dir, "${tableName}_index_${indexName}_${fieldName}_schema.avsc")

        val tableSchema = data.tableSchema

        val fieldType =
            tableSchema.fields.find { it.name() == fieldName }?.schema()?.type?.getName()
        indexFile.createNewFile()
        fieldSchemaFile.createNewFile()

        var builder = StringBuilder()
            .append("{")
            .append("\"name\":")
            .append("\"${tableName}\"")
            .append(",")
            .append("\"type\": \"record\"")
            .append(",")
            .append("\"fields\": [")
        builder = builder
            .append("{\"name\":")
            .append("\"${fieldName}\",")
            .append("\"type\": \"${fieldType}\",")
            .append("\"order\":")
            .append("\"ascending\"")
            .append("}")
            .append("]}")
        fieldSchemaFile.writeText(builder.toString())

        val fieldSchema = Schema.Parser().parse(fieldSchemaFile)

        val indexFieldSchema = Schema.Parser().parse(fieldSchemaFile)
        var indexFieldRecord = GenericRecord(fieldSchema)
        val keyCompare = indexFieldRecord.keyCompare

        val indexFileManager = IndexFileManager.new(indexFile)
        val indexPageManager = IndexPageManager(indexFileManager)

        val tree = BPlusTree(indexPageManager, keyCompare)

        data.heapPageManager.forEachRowPageIndexed { row, pageId ->
            val tableRecord = GenericRecord(tableSchema)
            tableRecord.load(row.value)
            val key = tableRecord.get(fieldName)

            indexFieldRecord = GenericRecord(indexFieldSchema)
            indexFieldRecord.put(fieldName, key)

            val indexValue = IndexValue(pageId, row.rowId!!)

            Log.d("TEST", "ADDING KEYVALUE: $indexValue")
            val keyValue =
                KeyValue(
                    indexFieldRecord.toByteBuffer(),
                    indexValue.toBytes()
                )
            tree.put(Record(keyValue))
        }

        indexFieldRecord = GenericRecord(indexFieldSchema)

        indexFieldRecord.put(fieldName, "DRAGAN") // all records with Ivan as the index key

        val record = Record(indexFieldRecord.toByteBuffer(), ByteBuffer.allocate(0))
        val res = tree.get(record)

        val values = IndexValues.fromBytes(res!!.value)

        values.records.forEach {
            Log.d("TEST", "res: $it")
        }

//
//        val records = tree.scan(indexFieldRecord.toByteBuffer())
//        records.forEach {
//            val testRes = IndexValue.fromBytes(it.value)
//
//            Log.d("TEST", "res: $testRes")
//        }
//        val value = res?.value
//
//        val testRes = IndexValue.fromBytes(value!!)
//
//        Log.d("TEST", "res: $testRes")
    }

    private fun loadTable(tableName: String) {
        if (!managerPool.contains(tableName)) {
            val file = File(dir, "$tableName.db")
            val schemaFile = File(dir, "$tableName.avsc")
            val fileManager = HeapFileManager.load(file)
            val heapPageManager = HeapPageManager(fileManager)
            managerPool[tableName] =
                TableManagerData(heapPageManager, Schema.Parser().parse(schemaFile), mutableMapOf())
        }
    }

    // guaranteed to be called after loadTable
    private fun loadIndex(tableName: String) {
        val indexManagers =
            initIndexManagersForTable(tableName)
        val data = managerPool[tableName]
        data!!.indexes = indexManagers
        managerPool[tableName] = data
    }

    private fun load(tableName: String) {
        loadTable(tableName)
        loadIndex(tableName)
    }

    // sample_index_birthdate_schema.avsc
    // tableName.db
    // tableName_index_name.index
    // tableName_index_name_schema.avsc
    private fun initIndexManagersForTable(tableName: String): Map<String, IndexData> {
        val indexes = mutableMapOf<String, IndexPageManager>()
        val compares = mutableMapOf<String, KeyCompare>()
        val schemas = mutableMapOf<String, Schema>()
        val managers = mutableMapOf<String, IndexData>()
        files.filter {
            it.startsWith("${tableName}_index")
        }.forEach {
            val file = File(dir, it)

            if (file.extension == "index") {
                val fileManager = IndexFileManager(File(dir, it))
                val indexPageManager = IndexPageManager(fileManager)
                val indexFieldName =
                    file.name.substringBeforeLast(".").split("_").last() // should be last - 1
                indexes[indexFieldName] = indexPageManager
            } else if (file.extension == "avsc") {
                val schema = Schema.Parser().parse(file)
                val record = GenericRecord(schema)
                val indexFieldName = file.name.substringBeforeLast(".").split("_").last()
                compares[indexFieldName] = record.keyCompare
                schemas[indexFieldName] = schema
            }
        }
        indexes.entries.forEach {
            val key = it.key
            val compare = compares[key]
            val schema = schemas[key]
            managers[key] = IndexData(schema!!, BPlusTree(it.value, compare!!))
        }
        return managers
    }

    fun insertRows(
        tableName: String, fields: MutableList<String>,
        inserts: MutableList<MutableList<String>>
    ) {
        load(tableName)
        val data = managerPool[tableName]!!
        val tableRows = mutableListOf<TableRow>()
        val schema = data.tableSchema
        val omittedFields = schema.fields.filter {
            !fields.contains(it.name())
        }
        inserts.removeAt(0) // first item it always empty due to way of parsing
        inserts.forEach {
            val record = GenericRecord(data.tableSchema)
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

    /*
    for the sake of simplicity, I won't write a goddamn query analyser to optimise usage of  indexes in complex
    queries with subexpressions. Way the fuck out of scope. Next time delve into the topic when you create assignments.

    AND -> return only the rows that exist in both index scan sets
    OR -> just merge the two record sets from index scan
     */
    // WHERE Id > 4 && Id < 6
    fun indexScan(tableName: String, op: WhereClauseType.LogicalOperation) {
        val data = managerPool[tableName]!!
        when (op.operator) {
            LogicalOperator.AND -> {
                val leftCond = op.leftNode
                val rightCond = op.rightNode
                val leftOperand1 = leftCond!!.operand1
                val rightOperand2 = rightCond!!.operand1
                if (leftOperand1 == rightOperand2) {
                    val indexFieldData = data.indexes[leftOperand1]!!
                    val record = GenericRecord(indexFieldData.indexSchema)
                    val tree = indexFieldData.tree
                    when (leftCond.operator) {
                        Operator.Undefined -> TODO()
                        Operator.Eq -> {
//                            val record =
//                            tree.get()
                        }
                        Operator.Ne -> TODO()
                        Operator.Gt -> TODO()
                        Operator.Lt -> TODO()
                        Operator.Gte -> TODO()
                        Operator.Lte -> TODO()
                    }
                } else {

                }
            }
            LogicalOperator.OR -> {
//                val indexScanLeftNodeRes =
//                val indexScanRightodeRes
            }
            LogicalOperator.NOT -> {

            }
            else -> {}
        }
    }

    fun applyIndexCondition(condition: WhereClauseType.Condition) {
        val data = managerPool[condition.operand1]!!
        val record =
            GenericRecord(data.indexes[condition.operand1]!!.indexSchema) // think if this is correct
        when (condition.operator) {
            Operator.Eq -> {

            }
            Operator.Ne -> {

            }
            Operator.Gt -> TODO()
            Operator.Lt -> TODO()
            Operator.Gte -> TODO()
            Operator.Lte -> TODO()
            Operator.Undefined -> {}
        }
    }

    fun fullTableScan() {

    }

    fun select(
        tableName: String,
        fields: List<String>,
        conditions: List<WhereClauseType.LogicalOperation>
    ): SelectResultModel {
        load(tableName)
        val data = managerPool[tableName]!!

        if (checkForPossibleIndexScan(fields, tableName) && conditions.size == 1) {
            indexScan(tableName, conditions.first())
        } else {
            fullTableScan()
        }

        var curPageId = ROOT_PAGE_ID

        val values: MutableList<MutableList<String>> = mutableListOf()

        loop@ while (true) {
            try {
                Log.d("TEST", "GETTING $curPageId")
                val page = data.heapPageManager.get(curPageId)
                Log.d("TEST", "records size: ${page!!.records.size}")

                page.records.forEach {
                    Log.d("TEST", "PAGE")
                    val record = GenericRecord(data.tableSchema)

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

    private fun checkForPossibleIndexScan(fields: List<String>, tableName: String): Boolean {
        val data = managerPool[tableName]!!
        fields.forEach {
            data.indexes[it] ?: return false
        }
        return true
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