package com.example.saa_project_db_engine

import java.nio.ByteBuffer

typealias KeyCompare = (ByteArray, ByteArray) -> Int

typealias MergeRule = (new: ByteBuffer, old: ByteBuffer) -> ByteBuffer