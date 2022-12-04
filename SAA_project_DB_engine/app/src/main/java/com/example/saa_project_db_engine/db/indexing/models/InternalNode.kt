package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.KeyCompare

open class InternalNode constructor(page: IndexLogicalPage, keyCompare: KeyCompare) :
    LeafNode(page, keyCompare) {
}