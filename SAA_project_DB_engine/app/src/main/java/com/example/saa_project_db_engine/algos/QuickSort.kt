package com.example.saa_project_db_engine.algos

import com.example.saa_project_db_engine.services.handlers.SelectResArray

fun quickSort(arr: SelectResArray): SelectResArray {
    sortArrayPart(arr, 0, arr.size - 1)
    return arr
}

private fun partition(arr: SelectResArray, fromIndex: Int, toIndex: Int): Int {
    val lastElementValue = arr[toIndex].first
    var i = fromIndex - 1
    for (j in fromIndex..toIndex - 1) {
        val swapHandler = {
            i++
            swap(arr, i, j)
        }
        arr[j].first.forEachIndexed { idx, value ->
            if (idx == arr[j].first.lastIndex) {
                if (value <= lastElementValue[idx]) {
                    swapHandler()
                }
            } else {
                if (value < lastElementValue[idx]) { // try strict check until you get to the last index. Then
                    swapHandler()
                }
            }
        }
    }
    swap(arr, i + 1, toIndex)
    return i + 1
}

private fun sortArrayPart(arr: SelectResArray, fromIndex: Int, toIndex: Int) {
    if (fromIndex < toIndex) {
        val middleIndex = partition(arr, fromIndex, toIndex)
        sortArrayPart(arr, fromIndex, middleIndex - 1)
        sortArrayPart(arr, middleIndex + 1, toIndex)
    }
}

private fun swap(arr: SelectResArray, i: Int, j: Int): SelectResArray {
    val tmp = arr[i]
    arr[i] = arr[j]
    arr[j] = tmp
    return arr
}