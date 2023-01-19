package com.example.saa_project_db_engine

import org.apache.avro.io.BinaryData
import java.nio.ByteBuffer
import java.util.*

fun ByteArray.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(this)

fun ByteArray.toHexString() = joinToString(":") { String.format("%02x", it) }

fun ByteBuffer.toByteArray(buffer: ByteArray? = null): ByteArray {
    position(0)
    val bytes = buffer ?: ByteArray(remaining())
    get(bytes, 0, remaining())
    rewind()
    return bytes
}

fun ByteBuffer.toAvroBytesSize(): Int {
    val size = limit()
    return size + size.toAvroBytesSize()
}

fun ByteBuffer.toHexString() = toByteArray().toHexString()

fun UInt.toAvroBytesSize(): Int {
    val bytes = ByteArray(5)
    return BinaryData.encodeInt(this.toInt(), bytes, 0)
}

fun Int.toAvroBytesSize(): Int {
    val bytes = ByteArray(5)
    return BinaryData.encodeInt(this, bytes, 0)
}

fun Int.toLengthAvroByteSize(): Int {
    return when (this) {
        0 -> BYTE_SIZE_0
        else -> BYTE_SIZE_0 + toAvroBytesSize()
    }
}

fun String.safeCapitalize(): String {
    return replaceFirstChar {
        if (it.isLowerCase()) it.titlecase(
            Locale.ROOT
        ) else it.toString()
    }
}

fun UByte.toBigEndianUInt(): UInt = this.toUInt() shl 24