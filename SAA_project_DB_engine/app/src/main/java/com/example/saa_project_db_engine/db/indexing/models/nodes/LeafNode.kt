package com.example.saa_project_db_engine.db.indexing.models.nodes

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.indexing.models.IndexLogicalPage

open class LeafNode constructor(page: IndexLogicalPage, keyCompare: KeyCompare) :
    Node(page, keyCompare) {
}