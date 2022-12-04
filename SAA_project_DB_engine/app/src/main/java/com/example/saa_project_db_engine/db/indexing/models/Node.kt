package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.MergeRule
import com.example.saa_project_db_engine.toByteArray
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

//    fun get(key: ByteBuffer): Record? {
//        val result = find(key)
//        return when (result) {
//            is FindResult.ExactMatch -> Record(key, result.value)
//            else -> null
//        }
//    }
//
//    fun put(key: ByteBuffer, value: ByteBuffer, merge: MergeRule? = null) {
//        when (val result = find(key)) {
//            is FindResult.ExactMatch -> {
//                val newValue = merge?.invoke(value, result.value) ?: value
//                page.update(result.index, KeyValue(key, newValue))
//            }
//            is FindResult.FirstGreaterThanMatch -> page.insert(KeyValue(key, value), result.index)
//            null -> page.insert(KeyValue(key, value), 0)
//        }
//    }

//    fun delete(key: ByteBuffer) {
//        val result = find(key)
//        if (result is FindResult.ExactMatch) {
//            page.delete(result.index)
//        }
//    }
//
//    fun get(record: Record) = get(record.key)
//    fun put(record: Record) = put(record.key, record.value)
//    fun delete(record: Record) = delete(record.key)

//    fun split(pageManager: PageManager): Page {
//        return pageManager.split(page)
//    }

//    fun commit(pageManager: PageManager) {
//        pageManager.commit(page)
//    }

//    fun printNode(pageManager: PageManager, indent: Int = 0) {
//        when (type) {
//            NodeType.LeafNode -> {
//                println(
//                    "${"    ".repeat(indent)}$type(id:$id size=$size keys=${
//                        records.map { it.key.toHexString() }.toList()
//                    })"
//                )
//            }
//            NodeType.InternalNode, NodeType.RootNode -> {
//                println("${"    ".repeat(indent)}$type(id:$id size=$size records=$recordsSize)")
//                records.forEach {
//                    val childPageId = InternalNode.decodeChildPageId(it.value)
//                    val childPage = pageManager.get(childPageId)!!
//                    val child = when (childPage.nodeType) {
//                        NodeType.InternalNode -> InternalNode(childPage, compare)
//                        NodeType.LeafNode -> LeafNode(childPage, compare)
//                        else -> throw Exception()
//                    }
//                    println("${"    ".repeat(indent + 1)}key=${it.key.toHexString()}")
//                    child.printNode(pageManager, indent + 1)
//                }
//            }
//        }
//    }

    protected sealed class FindResult {
        data class ExactMatch(val index: Int, val value: ByteBuffer) : FindResult()
        data class FirstGreaterThanMatch(val index: Int) : FindResult()
    }

//    protected fun find(keyByteBuffer: ByteBuffer): FindResult? {
//        if (page.records.isEmpty()) return null
//        val keyBytes = keyByteBuffer.toByteArray()
//        for ((index, keyValue) in page.records.withIndex()) {
//            val key = keyValue.key
//            if (key == Tree.logicalMinimumKey) continue
//            val compared = compare(key.toByteArray(), keyBytes)
//            when {
//                compared == 0 -> return FindResult.ExactMatch(index, keyValue.value)
//                compared > 0 -> return FindResult.FirstGreaterThanMatch(index)
//            }
//        }
//        return FindResult.FirstGreaterThanMatch(recordsSize)
//    }

}