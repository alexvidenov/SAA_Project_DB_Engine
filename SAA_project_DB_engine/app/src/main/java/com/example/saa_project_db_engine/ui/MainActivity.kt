package com.example.saa_project_db_engine.ui

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import android.util.Log
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.managers.file.FileManager
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.managers.page.PageManager
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.Schema

import java.io.File
import java.nio.ByteBuffer

class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        SchemasServiceLocator.ctx = this

        val testSchema = GenericRecord(Schema.Parser().parse(File(filesDir, "TestSchema.avsc")))

        testSchema.put("email", "testEmail")
        testSchema.put("password", "testPassword")

        val testSchema2 = GenericRecord(Schema.Parser().parse(File(filesDir, "TestSchema.avsc")))

        testSchema2.put("email", "testEmail2")
        testSchema2.put("password", "testPassword2")

        val testBuf = testSchema.toByteBuffer()
        val testBuf2 = testSchema2.toByteBuffer()

        val row =
            com.example.saa_project_db_engine.db.storage.models.TableRow(testBuf)

//        val row2 =
//            com.example.saa_project_db_engine.db.storage.models.TableRow(6, testBuf2)
//
//        val bytes = com.example.saa_project_db_engine.db.storage.models.HeapPageData(
//            1,
//            2,
//            3,
//            mutableListOf(row, row2)
//        ).toBytes()

//        val decode =
//            com.example.saa_project_db_engine.db.storage.models.HeapPageData.fromBytes(bytes)

//        val testSchema3 = GenericRecord(Schema.Parser().parse(File(filesDir, "TestSchema.avsc")))
//
//        decode.records.forEach {
//            Log.d("TESTRECORD", "${it.value}")
//            testSchema3.load(it.value as ByteBuffer)
//            Log.d("TESTRECORD", "${testSchema3.get("email")} ${testSchema3.get("password")}")
//        }

//        Log.d("TEST", "ARRAY: ${decode.records}")
//
//        Log.d("TEST", "decoded: ${decode.id} ${decode.previousPageId} ${decode.nextPageId}")


        val file = File(filesDir, "test.db")
        val schemaFile = File(filesDir, "TestSchema.avsc")

        val fileManager = HeapFileManager.new(file)
        val pageManager = HeapPageManager(fileManager)

        val schema = Schema.Parser().parse(schemaFile)

        val record = GenericRecord(schema)

        pageManager.insertRow(row)

        val page = pageManager.get(ROOT_PAGE_ID)

        page!!.records.forEach {
            record.load(it.value)
            val email = record.get("email")
            val pass = record.get("password")
//            Log.d("TEST", "EMAIL: $email")
//            Log.d("TEST", "PASS: $pass")
        }

        val row2 =
            com.example.saa_project_db_engine.db.storage.models.TableRow(testBuf)

        pageManager.insertRow(row2)

        val page2 = pageManager.get(ROOT_PAGE_ID)

        page2!!.records.forEach {
            Log.d("TEST", "row id is ${it.rowId}")
            record.load(it.value)
            val email = record.get("email")
            val pass = record.get("password")
            Log.d("TEST", "EMAIL: $email")
            Log.d("TEST", "PASS: $pass")
        }

    }
}