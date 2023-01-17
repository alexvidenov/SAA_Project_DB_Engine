package com.example.saa_project_db_engine.algos

import com.example.saa_project_db_engine.services.handlers.SelectResArray

fun quickSort(array: SelectResArray, left: Int, right: Int) {
    val index = partition(array, left, right)
    if (left < index - 1) {
        quickSort(array, left, index - 1)
    }
    if (index < right) {
        quickSort(array, index, right)
    }
}

fun partition(array: SelectResArray, l: Int, r: Int): Int {
    var left = l
    var right = r
    val pivot = array[(left + right) / 2].first
    while (left <= right) {
        while (array[left].first < pivot) left++

        while (array[right].first > pivot) right--

        if (left <= right) {
            swapArrayElements(array, left, right)
            left++
            right--
        }
    }
    return left
}

fun swapArrayElements(a: SelectResArray, b: Int, c: Int) {
    val temp = a[b]
    a[b] = a[c]
    a[c] = temp
}