package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager

class RootNode(page: IndexLogicalPage, compare: KeyCompare) : InternalNode(page, compare) {
//    fun splitRoot(pageManager: IndexPageManager): Pair<IndexLogicalPage, IndexLogicalPage> {
//        val newType = if (type == NodeType.RootNode) NodeType.InternalNode else NodeType.LeafNode
////        val leftPage = pageManager.move(page, newType, 0 until recordsSize)
////        val rightPage = pageManager.split(leftPage)
//        return Pair(leftPage, rightPage)
//    }
}