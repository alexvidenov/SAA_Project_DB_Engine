package com.example.saa_project_db_engine.services

// SITUATION:
// DELETE FROM TABLE WHERE {non-indexed column}.
// affects certain rowIds. You need to update the indec with the affected row ids.
// Meaning you need index organized table around row Ids ? Right.

// TODO: if not having enough time, simulate it with implicitly created index on rowId.

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.NullPersistenceModelException
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
import kotlin.math.log

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

            Log.d("TEST", "FREAKING SCHEMA IN CREATEINDEX: ${indexFieldSchema}")
            indexFieldRecord = GenericRecord(indexFieldSchema)
            Log.d("TEST", "PUTTING: ${fieldName} ${key}")
            indexFieldRecord.put(fieldName, key) // ut8 bullshit??

            val indexValue = IndexValue(pageId, row.rowId!!)

            Log.d("TEST", "ADDING KEYVALUE: $indexValue")
            val keyValue =
                KeyValue(
                    indexFieldRecord.toByteBuffer(),
                    indexValue.toBytes()
                )
            tree.put(IndexRecord(keyValue))
        }

        indexFieldRecord = GenericRecord(indexFieldSchema)

        indexFieldRecord.put(fieldName, 3) // all records with Ivan as the index key

        val record = IndexRecord(indexFieldRecord.toByteBuffer(), ByteBuffer.allocate(0))
        val res = tree.get(record)

        val values = IndexValues.fromBytes(res!!.value)

        values.records.forEach {
            Log.d("TEST", "SOMEHOW THIS FREAKING WORKS: $it")
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
            Log.d("TEST", "file: ${file.name}")
            if (file.extension == "index") {
                val fileManager = IndexFileManager.load(file)
                val indexPageManager = IndexPageManager(fileManager)
                val indexFieldName =
                    file.name.substringBeforeLast(".").split("_").last()
                Log.d(
                    "TEST",
                    "indexFieldName: ${indexFieldName}. AND FUCKING SIZE: ${indexPageManager.get(0)?.records?.size}"
                )
                indexes[indexFieldName] = indexPageManager
            } else if (file.extension == "avsc") {
                val schema = Schema.Parser().parse(file)
                val record = GenericRecord(schema)
                Log.d("TEST", "SCHEMA IN initIndexManagersForTable ${schema}")
                val split = file.name.substringBeforeLast(".").split("_")
                val indexFieldName = split[split.size - 2]
                Log.d("TEST", "indexFieldName: $indexFieldName")
                compares[indexFieldName] = record.keyCompare
                schemas[indexFieldName] = schema
            }
        }
        indexes.entries.forEach {
            val key = it.key
            val compare = compares[key]
            val schema = schemas[key]
            Log.d("TEST", "KILL SOMETHING: ${compare} ${schema}")
            val tree = BPlusTree(it.value, compare!!)

            val indexFieldRecord = GenericRecord(schema!!)

            indexFieldRecord.put("Id", 3) // all records with Ivan as the index key

            val record = IndexRecord(indexFieldRecord.toByteBuffer(), ByteBuffer.allocate(0))
            val res = tree.get(record)

            val values = IndexValues.fromBytes(res!!.value)

            values.records.forEach {
                Log.d("TEST", "res from FUCKING HELL: $it")
            }
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
    private fun indexScan(tableName: String, op: WhereClauseType.LogicalOperation): IndexValues? {
        val data = managerPool[tableName]!!
        when (op.operator) {
            LogicalOperator.AND -> {
                val leftCond = op.leftNode
                val rightCond = op.rightNode
                val leftOperand1 = leftCond!!.operand1
                val rightOperand1 = rightCond!!.operand1
                var lower: ByteBuffer = ByteBuffer.allocate(0)
                var upper: ByteBuffer = ByteBuffer.allocate(0)
                // excludes automatically the case of equality (for the sake of simplicity, again)
                if (leftOperand1 == rightOperand1) {
                    val indexData = data.indexes[leftOperand1]
                    val record = GenericRecord(indexData!!.indexSchema)
                    record.put(
                        leftOperand1,
                        convertOperandToNativeType(leftCond.operand2, leftOperand1, record)
                    )
                    when (leftCond.operator) {
                        Operator.Gt -> lower = record.toByteBuffer()
                        Operator.Lt -> upper = record.toByteBuffer()
                        Operator.Gte -> lower = record.toByteBuffer()
                        Operator.Lte -> upper = record.toByteBuffer()
                        else -> {}
                    }
                    return applyBoundedIndexScanCondition(leftOperand1, lower, upper)
                } else {
                    Log.d("TEST", "NOT EQUAL")
                    val leftRes = applyIndexCondition(tableName, leftCond)
                    Log.d("TEST", "LEFT RES: ${leftRes}")
                    val rightRes = applyIndexCondition(tableName, leftCond)
                    Log.d("TEST", "RIGHT RES: ${rightRes}")
                    return if (leftRes != null && rightRes != null) {
                        val filteredRecords = mutableListOf<IndexValue>()
                        leftRes.records.forEach {
                            if (rightRes.records.contains(it)) {
                                filteredRecords.add(it)
                            }
                        }
                        IndexValues(filteredRecords)
                    } else null
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
        return null
    }

    private fun indexScan(tableName: String, op: WhereClauseType.Condition): IndexValues? {
        Log.d("TEST", "applyIndexCondition")
        return applyIndexCondition(tableName, op)
    }

    private fun fetchHeapResultsFromIndexValues(
        tableName: String,
        fields: List<String>,
        values: IndexValues
    ): SelectResultModel {
        val returnModel: MutableList<MutableList<String>> = mutableListOf()
        val data = managerPool[tableName]!! // CURRENT TABLE FOR EXAMPLE
        values.records.forEach {
            val row = fetchRowFromIndexValue(tableName, it)
            val array = mutableListOf<String>()
            val record = GenericRecord(data.tableSchema)
            record.load(row.value)
            array.add(row.rowId.toString())
            fields.forEach { f -> // TODO: LATEST ARGS QUEUE AGAIN
                Log.d("TEST", "FIELD: $f")
                val field = record.get(f)
                Log.d("TEST", "FIELD2: $field")
                array.add(field.toString())
            }
            returnModel.add(array)
        }
        return SelectResultModel(fields, returnModel)
    }

    // assuming the index stays consistent. (which it does, for now)
    private fun fetchRowFromIndexValue(tableName: String, index: IndexValue): TableRow {
        val data = managerPool[tableName]!!
        // TODO: seriously, instead of passing shit all over hte place, have a queue with a data
        // class that represents different args
        val heapManager = data.heapPageManager
        val page = heapManager.get(index.pageId)
        val recordIndex = page!!.getIndexForRowId(index.rowId)
        return page.records[recordIndex!!]
    }

    private fun convertOperandToNativeType(
        value: String,
        operand: String,
        record: GenericRecord
    ): Any {
        return convertStringToNativeType(
            value,
            fieldType(record, operand)
        )
    }

    private fun applyIndexCondition(
        tableName: String,
        condition: WhereClauseType.Condition
    ): IndexValues? {
        val data = managerPool[tableName]!!
        val operand1 = condition.operand1
        val operand2 = condition.operand2
        Log.d("TEST", "OPERAND1: $operand1")
        Log.d("TEST", "OPERAND2: $operand2")
        val indexData = data.indexes[operand1]!!
        val schema = indexData.indexSchema
        val record =
            GenericRecord(schema)
        Log.d(
            "TEST",
            "OP1: $operand1, converted: ${convertOperandToNativeType(operand2, operand1, record)}"
        )
        record.put(
            operand1,
            convertOperandToNativeType(operand2, operand1, record)
        )
        val indexRecord = IndexRecord(record.toByteBuffer(), ByteBuffer.allocate(0))
        val tree = indexData.tree
        Log.d(
            "TEST",
            "TREE: ${tree.debug()}"
        )
        when (condition.operator) {
            Operator.Eq -> {
                val res = tree.get(indexRecord)
                if (res != null) {
                    return IndexValues.fromBytes(res.value)
                }
                return null
            }
            Operator.Ne -> TODO()
            Operator.Gt -> {
                val res = tree.scan(startKey = record.toByteBuffer())
                return sequenceToIndexRecords(res)
            }
            Operator.Lt -> {
                val res = tree.scan(endKey = record.toByteBuffer())
                return sequenceToIndexRecords(res)
            }
            Operator.Gte -> TODO()
            Operator.Lte -> TODO()
            else -> {}
        }
        return null
    }

    private fun applyBoundedIndexScanCondition(
        operandName: String,
        lower: ByteBuffer,
        upper: ByteBuffer
    ): IndexValues? {
        // TODO: extract this shit, please
        val data = managerPool[operandName]!!
        val indexData = data.indexes[operandName]!!
        val records = indexData.tree.scan(lower, upper)
        return sequenceToIndexRecords(records)
    }

    private fun sequenceToIndexRecords(records: Sequence<IndexRecord>): IndexValues? {
        val indexValuesReturn = mutableListOf<IndexValue>()
        records.forEach {
            val indexValues = IndexValues.fromBytes(it.value)
            indexValuesReturn.addAll(indexValues.records)
        }
        return if (indexValuesReturn.isNotEmpty()) {
            IndexValues(indexValuesReturn)
        } else null
    }

    private fun fullTableScan(
        tableName: String,
        fields: List<String>,
        conditions: List<WhereClauseType.LogicalOperation>
    ): SelectResultModel {
        val data = managerPool[tableName]!!

        val values: MutableList<MutableList<String>> = mutableListOf()

        data.heapPageManager.forEachRowPageIndexed { row, pageId ->
            val record = GenericRecord(data.tableSchema)
            record.load(row.value)

            val res = parseSubExpression(record, conditions)

            Log.d("EXPR", "R0W RES: ${res}")
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

//        // extract this as well
//        loop@ while (true) {
//            try {
//                Log.d("TEST", "GETTING $curPageId")
//                val page = data.heapPageManager.get(curPageId)
//                Log.d("TEST", "records size: ${page!!.records.size}")
//
//                page.records.forEach {
//                    Log.d("TEST", "PAGE")
//                    val record = GenericRecord(data.tableSchema)
//
//                    Log.d("TEST", "${it.value} ${it.rowId}")
//                    record.load(it.value)
//
//                    val res = parseCondition(record)
//
//                    val res = parseSubExpression(record, conditions)
//
//                    if (res) {
//                        val array = mutableListOf<String>()
//                        fields.forEach { f ->
//                            Log.d("TEST", "FIELD: $f")
//                            val field = record.get(f)
//                            Log.d("TEST", "FIELD2: $field")
//                            array.add(field.toString())
//                        }
//                        values.add(array)
//                    }
//                }
//
//                curPageId++
//            } catch (e: NullPersistenceModelException) {
//                Log.d("TEST", "BREAKING LOOP LABEL")
//                break@loop
//            }
//        }

        val model = SelectResultModel(fields, values)
        Log.d("TEST", "SELECT RESULT MODEL: $model")

        return model
    }

    private fun fullTableScan(
        tableName: String,
        fields: List<String>, cond: WhereClauseType.Condition
    ): SelectResultModel {
        val data = managerPool[tableName]!!

        val values: MutableList<MutableList<String>> = mutableListOf()

        data.heapPageManager.forEachRowPageIndexed { row, pageId ->
            val record = GenericRecord(data.tableSchema)
            record.load(row.value)

            val res = parseCondition(record, cond)

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

        val model = SelectResultModel(fields, values)
        Log.d("TEST", "SELECT RESULT MODEL: $model")

        return model
    }

    fun select(
        tableName: String,
        fields: List<String>, whereFields: List<String>, currentCond: WhereClauseType.Condition
    ): SelectResultModel {
        load(tableName)
        return if (analysePossibleIndexScan(whereFields, tableName)) {
            Log.d("TEST", "INDEX SCAN")
            val indexes = indexScan(tableName, currentCond)
            if (indexes != null) {
                Log.d("TEST", "fetchHeapResultsFromIndexValues")
                fetchHeapResultsFromIndexValues(tableName, fields, indexes)
            } else {
                Log.d("TEST", "null index results")
                SelectResultModel(fields, mutableListOf())
            }
        } else {
            Log.d("TEST", "FULL TABLE SCAN")
            fullTableScan(tableName, fields, currentCond)
        }
    }

    fun select(
        tableName: String,
        fields: List<String>,
        whereFields: List<String>,
        conditions: List<WhereClauseType.LogicalOperation>,
    ): SelectResultModel {
        load(tableName)
        return if (analysePossibleIndexScan(whereFields, tableName) && conditions.size == 1) {
            Log.d("TEST", "INDEX SCAN")
            val indexes = indexScan(tableName, conditions.first())
            if (indexes != null) {
                Log.d("TEST", "fetchHeapResultsFromIndexValues")
                fetchHeapResultsFromIndexValues(tableName, fields, indexes)
            } else {
                Log.d("TEST", "null index results")
                SelectResultModel(fields, mutableListOf())
            }
        } else {
            Log.d("TEST", "FULL TABLE SCAN")
            fullTableScan(tableName, fields, conditions)
        }
    }

    private fun analysePossibleIndexScan(fields: List<String>, tableName: String): Boolean {
        val data = managerPool[tableName]!!
        Log.d("TEST", "indexes: ${data.indexes}")
        fields.forEach {
            Log.d("TEST", "SCAN TEST STRING: ${it}")
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
        Log.d("EXPR", "operations: $ops")
        while (i <= ops.size - 1) {
            val op = ops[i]
            Log.d("EXPR", "operation: $op")
            currentOpType = op.operator
            val parseState = parseLogicalOperation(record, op)
            Log.d("EXPR", "Empty state: ${parseState.empty}")
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
            Log.d("EXPR", "currentCachedOperationResult: $currentCachedOperationResult")
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
            Log.d("EXPR", "parsed left node: $left")
        }
        op.rightNode?.let {
            right = parseCondition(record, it)
            Log.d("EXPR", "parsed right node: $right")
        }
        op.leftSubExpr?.let {
            left = parseSubExpression(record, it)
            Log.d("EXPR", "parsed left subexpr: $left")
        }
        op.rightSubExpr?.let {
            right = parseSubExpression(record, it)
            Log.d("EXPR", "parsed right subexpr: $right")
        }

        return if (left != null && right != null) {
            val res = applyOperation(left, right, op.operator!!)
            Log.d("EXPR", "after applying operation: $res")
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
        Log.d("EXPR", "PARSE CONDITION: ${cond}")
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
            "EXPR",
            "CONDITION: \n$op1\n$op2\n${cond.operator} $op1 ${op1!!.javaClass.name} $op2 ${op2!!.javaClass.name}"
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