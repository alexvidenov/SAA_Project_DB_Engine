package com.example.saa_project_db_engine.algos

import com.example.saa_project_db_engine.toBigEndianUInt

@OptIn(ExperimentalUnsignedTypes::class)
class CRC32(private val polynomial: UInt = 0x04C11DB7.toUInt()) {
    val lookupTable: List<UInt> = (0 until 256).map { crc32(it.toUByte(), polynomial) }

    var value: UInt = 0.toUInt()
        private set

    fun update(inputs: UByteArray) {
        value = crc32(inputs, value)
    }

    fun reset() {
        value = 0.toUInt()
    }

    // remainder of a division of the input and the polynomial
    private fun crc32(inputs: UByteArray, initialValue: UInt = 0.toUInt()): UInt {
        return inputs.fold(initialValue) { remainder, byte ->
            val bigEndianInput = byte.toBigEndianUInt()
            val index = (bigEndianInput xor remainder) shr 24
            lookupTable[index.toInt()] xor (remainder shl 8)
        }
    }

    private fun crc32(input: UByte, polynomial: UInt): UInt {
        val bigEndianInput = input.toBigEndianUInt()

        return (0 until 8).fold(bigEndianInput) { result, _ ->
            val isMostSignificantBitOne = result and 0x80000000.toUInt() != 0.toUInt()
            val shiftedResult = result shl 1

            when (isMostSignificantBitOne) {
                true -> shiftedResult xor polynomial // subtract the polynomial from the result.
                false -> shiftedResult
            }
        }
    }
}