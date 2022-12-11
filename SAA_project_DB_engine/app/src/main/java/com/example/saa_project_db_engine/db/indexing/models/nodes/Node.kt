package com.example.saa_project_db_engine.db.indexing.models.nodes

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.indexing.models.BPlusTree
import com.example.saa_project_db_engine.db.indexing.models.IndexLogicalPage
import com.example.saa_project_db_engine.db.indexing.models.KeyValue
import com.example.saa_project_db_engine.db.indexing.models.Record
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
    val records get() = page.records.asSequence().map { Record(it) }
    val recordsReversed get() = page.records.reversed().asSequence().map { Record(it) }

    val recordsSize get() = page.records.size
    val minRecord get() = records.first()

    fun dump() = page.dump()

    fun isLeafNode(): Boolean = type == NodeType.LeafNode
    fun isInternalNode(): Boolean = type == NodeType.InternalNode
    fun isRootNode(): Boolean = type == NodeType.RootNode

    fun get(key: ByteBuffer): List<Record>? {
        return when (val result = find(key)) {
            is FindResult.ExactMatches -> {
                result.indexValueMap.map {
                    val entry = it.value
                    Record(entry.key, entry.value)
                }
            }
            else -> null
        }
    }

    fun put(key: ByteBuffer, value: ByteBuffer) {
        when (val result = find(key)) {
            is FindResult.ExactMatches -> {
                page.insert(
                    KeyValue(key, value)
//                    result.indexValueMap.entries.last().key
                ) // TODO: + 1?
//                page.update(result.index, KeyValue(key, value))
            }
            is FindResult.FirstGreaterThanMatch -> page.insert(KeyValue(key, value), result.index)
            null -> page.insert(KeyValue(key, value), 0)
            else -> {}
        }
    }

    fun delete(key: ByteBuffer) {
        val result = find(key)
        if (result is FindResult.ExactMatches) {
            result.indexValueMap.entries.forEach {
                page.delete(it.key, it.value)
            }
        }
    }

    fun get(record: Record) = get(record.key)
    fun put(record: Record) = put(record.key, record.value)
    fun delete(record: Record) = delete(record.key)

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
        data class ExactMatches(val indexValueMap: Map<Int, KeyValue>) : FindResult()
        data class FirstGreaterThanMatch(val index: Int) : FindResult()
    }

    protected fun find(keyByteBuffer: ByteBuffer): FindResult? {
        if (page.records.isEmpty()) return null
        val keyBytes = keyByteBuffer.toByteArray()
        val matches = mutableMapOf<Int, KeyValue>()
        for ((index, keyValue) in page.records.withIndex()) {
            val key = keyValue.key
            if (key == BPlusTree.logicalMinimumKey) continue
            val compared = compare(
                key.toByteArray(),
                keyBytes
            ) // compares only the search term, the pageId + rowId is obviously not compared
            when {
                compared == 0 -> {
                    matches[index] = keyValue
                }
                compared > 0 -> {
                    if (matches.isEmpty()) {
                        return FindResult.FirstGreaterThanMatch(index)
                    }
                }
            }
        }
        if (matches.isNotEmpty()) {
            return FindResult.ExactMatches(matches)
        }
        return FindResult.FirstGreaterThanMatch(recordsSize)
    }

}