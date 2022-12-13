package com.example.saa_project_db_engine.db.indexing.models

import android.util.Log
import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.PageFullException
import com.example.saa_project_db_engine.db.indexing.models.nodes.*
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.toByteArray
import com.example.saa_project_db_engine.toByteBuffer
import java.lang.Exception
import java.nio.ByteBuffer

class BPlusTree(private val pageManager: IndexPageManager, private val keyCompare: KeyCompare) {
    companion object {
        val logicalMinimumKey = byteArrayOf().toByteBuffer()
        val logicalMaximumKey = null
    }

    private val rootNode: RootNode = RootNode(pageManager.getRootPage(), keyCompare)
    private fun compare(a: ByteBuffer, b: ByteBuffer) = keyCompare(a.toByteArray(), b.toByteArray())

    fun get(key: ByteBuffer): IndexRecord? {
        val result = findLeafNode(key)
        return result.leafNode.get(key)
    }

    fun put(key: ByteBuffer, value: ByteBuffer) {
        val result = findLeafNode(key)
        try {
            result.leafNode.put(key, value)
            result.leafNode.commit(pageManager)
        } catch (e: PageFullException) {
            splitNode(result.leafNode, result.pathFromRoot.reversed().iterator())
        }
    }

    private fun delete(key: ByteBuffer) {
        val result = findLeafNode(key)
        result.leafNode.delete(key)
        result.leafNode.commit(pageManager)
    }

    fun get(record: IndexRecord) = get(record.key)
    fun put(record: IndexRecord) = put(record.key, record.value)
    fun delete(record: IndexRecord) = delete(record.key)

    fun scan(
        startKey: ByteBuffer? = logicalMinimumKey,
        endKey: ByteBuffer? = logicalMaximumKey
    ): Sequence<IndexRecord> {
        if (startKey == endKey) return sequenceOf()
        val isAscending = when {
            startKey == logicalMinimumKey -> true
            startKey == logicalMaximumKey -> false
            endKey == logicalMinimumKey -> false
            endKey == logicalMaximumKey -> true
            else -> compare(startKey, endKey) < 0
        }
        val firstNode = findLeafNode(startKey).leafNode
        val lastNode = findLeafNode(endKey).leafNode
        return if (isAscending) {
            generateSequence(firstNode) { if (it == lastNode) null else createLeafNode(it.nextId) }
                .flatMap { it.records }
                .dropWhile {
                    startKey != logicalMinimumKey && compare(
                        it.key,
                        startKey!!
                    ) < 0
                } // it < startKey
                .takeWhile {
                    endKey == logicalMaximumKey || compare(
                        it.key,
                        endKey
                    ) <= 0 // TODO: use that for GTE / GT
                }   // it <= endKey
        } else {
            generateSequence(firstNode) { if (it == lastNode) null else createLeafNode(it.previousId) }
                .flatMap { it.recordsReversed }
                .dropWhile {
                    startKey != logicalMaximumKey && compare(
                        it.key,
                        startKey
                    ) > 0
                } // it > startKey
                .takeWhile {
                    endKey == logicalMinimumKey || compare(
                        it.key,
                        endKey!!
                    ) >= 0
                }  // it >= endKey
        }
    }

    private fun createLeafNode(id: Int?): LeafNode? {
        if (id != null) {
            val page = pageManager.get(id) ?: return null
            return LeafNode(page, keyCompare)
        }
        return null
    }

    private data class FindResult(val leafNode: LeafNode, val pathFromRoot: List<InternalNode>)

    private fun findLeafNode(
        key: ByteBuffer?,
        parentNode: InternalNode = rootNode, pathFromRoot: List<InternalNode> = listOf()
    ): FindResult {
        if (parentNode.isLeafNode()) {
            return FindResult(parentNode, pathFromRoot) // No root yet
        }
        val newPathFromRoot = pathFromRoot + parentNode
        val childPageId = when (key) {
            logicalMinimumKey -> parentNode.firstChildPageId()
            logicalMaximumKey -> parentNode.lastChildPageId()
            else -> parentNode.findChildPageId(key)
        }
        val childPage = pageManager.get(childPageId) ?: throw Exception()
        return when (childPage.data.nodeType) {
            NodeType.LeafNode -> FindResult(LeafNode(childPage, keyCompare), newPathFromRoot)
            NodeType.InternalNode -> findLeafNode(
                key,
                InternalNode(childPage, keyCompare),
                newPathFromRoot
            )
            else -> throw Exception()
        }
    }

    private fun splitNode(node: Node, pathToRoot: Iterator<InternalNode>) {
        if (pathToRoot.hasNext()) {
            val parent = pathToRoot.next()
            val newNode = when (node.type) {
                NodeType.LeafNode -> LeafNode(node.split(pageManager), keyCompare)
                NodeType.InternalNode -> InternalNode(node.split(pageManager), keyCompare)
                else -> throw Exception()
            }
            newNode.commit(pageManager)
            try {
                parent.addChildNode(newNode)
                parent.commit(pageManager)
            } catch (e: PageFullException) {
                splitNode(parent, pathToRoot)
            }
        } else {
            splitRootNode()
        }
    }

    private fun splitRootNode() {
        val (leftPage, rightPage) = rootNode.splitRoot(pageManager)
        val (leftNode, rightNode) = when (rootNode.type) {
            NodeType.LeafNode -> Pair(
                LeafNode(leftPage, keyCompare),
                LeafNode(rightPage, keyCompare)
            )
            NodeType.RootNode -> Pair(
                InternalNode(leftPage, keyCompare),
                InternalNode(rightPage, keyCompare)
            )
            else -> throw Exception() // TODO
        }
        rootNode.addChildNode(leftNode, logicalMinimumKey)
        rootNode.addChildNode(rightNode)
        rootNode.type = NodeType.RootNode
        leftNode.commit(pageManager)
        rightNode.commit(pageManager)
        rootNode.commit(pageManager)
    }

    fun debug() {
        rootNode.printNode(pageManager)
    }
}