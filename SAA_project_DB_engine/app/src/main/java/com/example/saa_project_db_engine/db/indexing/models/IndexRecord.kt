package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.toByteBuffer
import java.nio.ByteBuffer

data class IndexRecord(val key: ByteBuffer, val value: ByteBuffer) {
    constructor(keyValue: KeyValue) : this(keyValue.key, keyValue.value)
    constructor(key: ByteArray, value: ByteArray) : this(key.toByteBuffer(), value.toByteBuffer())
    constructor(key: ByteBuffer, value: ByteArray) : this(key, value.toByteBuffer())
    constructor(key: ByteArray, value: ByteBuffer) : this(key.toByteBuffer(), value)
}