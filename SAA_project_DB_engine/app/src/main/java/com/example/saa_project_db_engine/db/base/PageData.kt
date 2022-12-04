package com.example.saa_project_db_engine.db.base

import java.nio.ByteBuffer

abstract class PageData<R : WithByteUtils> : SchemaAware() {
    abstract val id: Int
    abstract var previousPageId: Int
    abstract var nextPageId: Int

    abstract val records: MutableList<R>

    abstract fun toBytes(): ByteBuffer
}