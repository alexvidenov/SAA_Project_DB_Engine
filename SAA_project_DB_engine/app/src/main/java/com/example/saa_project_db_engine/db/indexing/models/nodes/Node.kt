package com.example.saa_project_db_engine.db.indexing.models.nodes

import android.util.Log
import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.MergeRule
import com.example.saa_project_db_engine.db.indexing.models.*
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.toByteArray
import com.example.saa_project_db_engine.toHexString
import java.nio.ByteBuffer

abstract class Node constructor(
    protected val page: IndexLogicalPage,
    protected val compare: KeyCompare
) {

    val id get() = page.id

    var type
        get() = page.data.nodeType
        set(value) {
            page.data.nodeType = value
        }

    var previousId
        get() = page.previousId
        set(value) {
            page.previousId = value
        }

    var nextId
        get() = page.nextId
        set(value) {
            page.nextId = value
        }

    val size get() = page.size
    val records get() = page.records.asSequence().map { IndexRecord(it) }
    val recordsReversed get() = page.records.reversed().asSequence().map { IndexRecord(it) }

    val recordsSize get() = page.records.size
    val minRecord get() = records.first()

    private val mergeRule: MergeRule = { new, old ->
        val oldValues = IndexValues.fromBytes(old)
        oldValues.records.forEach {
            Log.d("TEST", "OLD VALUE: ${it}")
        }
        val newValue = IndexValue.fromBytes(new)
        oldValues.records.add(newValue)
        Log.d("TEST", "MERGE RES: ${oldValues.records}")
        oldValues.toBytes()
    }

    fun dump() = page.dump()

    fun isLeafNode(): Boolean = type == NodeType.LeafNode
    fun isInternalNode(): Boolean = type == NodeType.InternalNode
    fun isRootNode(): Boolean = type == NodeType.RootNode

    fun get(key: ByteBuffer): IndexRecord? {
        return when (val result = find(key)) {
            is FindResult.ExactMatch -> {
                IndexRecord(key, result.keyValue.value)
            }
            else -> null
        }
    }

    fun put(key: ByteBuffer, value: ByteBuffer, merge: MergeRule? = null) {
        Log.d("TEST", "BTREE PUT")
        val valueToPersist = IndexValues(mutableListOf(IndexValue.fromBytes(value))).toBytes()
        when (val result = find(key)) {
            is FindResult.ExactMatch -> {
                val rule = merge ?: mergeRule
                val newValue = rule.invoke(value, result.keyValue.value)
                page.update(
                    result.index,
                    KeyValue(key, newValue)
                )
            }
            is FindResult.FirstGreaterThanMatch -> {
                Log.d("TEST", "FirstGreaterThanMatch: ${result.index}")
                page.insert(
                    KeyValue(key, valueToPersist),
                    result.index
                )
            }
            null -> {
                page.insert(KeyValue(key, valueToPersist), 0)
            }
        }
    }

    // TODO: instead of directly passing pageid and rowId, refactor to use IndexValue struct
    fun delete(key: ByteBuffer, pageId: Int, rowId: Int) {
        val result = find(key)
        if (result is FindResult.ExactMatch) {
            val indexValues = IndexValues.fromBytes(result.keyValue.value)
            val indexValue = IndexValue(pageId, rowId)
            indexValues.records.remove(indexValue)
            if (indexValues.records.isEmpty()) {
                page.delete(result.index, result.keyValue)
            } else {
                page.update(result.index, KeyValue(key, indexValues.toBytes()))
            }
        }
    }

    fun get(record: IndexRecord) = get(record.key)
    fun put(record: IndexRecord) = put(record.key, record.value)
    fun delete(record: IndexRecord, pageId: Int, rowId: Int) = delete(record.key, pageId, rowId)

    fun split(pageManager: IndexPageManager): IndexLogicalPage {
        return pageManager.split(page)
    }

    fun commit(pageManager: IndexPageManager) {
        pageManager.commit(page)
    }

    fun printNode(pageManager: IndexPageManager, indent: Int = 0) {
        when (type) {
            NodeType.LeafNode -> {
                println(
                    "${"    ".repeat(indent)}$type(id:$id size=$size keys=${
                        records.map { it.key.toHexString() }.toList()
                    })"
                )
            }
            NodeType.InternalNode, NodeType.RootNode -> {
                println("${"    ".repeat(indent)}$type(id:$id size=$size records=$recordsSize)")
                records.forEach {
                    val childPageId = InternalNode.decodeChildPageId(it.value)
                    val childPage = pageManager.get(childPageId)!!
                    val child = when (childPage.data.nodeType) {
                        NodeType.InternalNode -> InternalNode(childPage, compare)
                        NodeType.LeafNode -> LeafNode(childPage, compare)
                        else -> throw Exception()
                    }
                    println("${"    ".repeat(indent + 1)}key=${it.key.toHexString()}")
                    child.printNode(pageManager, indent + 1)
                }
            }
        }
    }

    protected sealed class FindResult {
        data class ExactMatch(val index: Int, val keyValue: KeyValue) : FindResult()
        data class FirstGreaterThanMatch(val index: Int) : FindResult()
    }

    protected fun find(keyByteBuffer: ByteBuffer): FindResult? {
        if (page.records.isEmpty()) return null
        val keyBytes = keyByteBuffer.toByteArray()
        for ((index, keyValue) in page.records.withIndex()) {
            val key = keyValue.key
            if (key == BPlusTree.logicalMinimumKey) continue
            val compared = compare(
                key.toByteArray(),
                keyBytes
            )
            when {
                compared == 0 -> {
                    Log.d("TEST", "EXACT MATCH")
                    return FindResult.ExactMatch(index, keyValue)
                }
                compared > 0 -> {
                    Log.d("TEST", "greater than MATCH")
                    return FindResult.FirstGreaterThanMatch(index)
                }
            }
        }
        Log.d("TEST", "greater than MATCH AT END OF FUNC")
        return FindResult.FirstGreaterThanMatch(recordsSize)
    }

}