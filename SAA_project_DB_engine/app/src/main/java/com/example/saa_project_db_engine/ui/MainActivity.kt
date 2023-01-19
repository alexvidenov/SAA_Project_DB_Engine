package com.example.saa_project_db_engine.ui

import android.os.Bundle
import android.widget.EditText
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.executor.SchemaExecutor
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import com.example.saa_project_db_engine.services.models.TableInfo
import com.example.saa_project_db_engine.ui.models.TableUIModel
import com.google.android.material.floatingactionbutton.FloatingActionButton
import com.google.android.material.textfield.TextInputLayout
import de.codecrafters.tableview.TableView
import de.codecrafters.tableview.toolkit.SimpleTableDataAdapter
import de.codecrafters.tableview.toolkit.SimpleTableHeaderAdapter
import kotlinx.coroutines.launch

class MainActivity : AppCompatActivity() {
    private lateinit var tableView: TableView<Array<String>>
    private lateinit var queryEditText: EditText
    private lateinit var textInputLayout: TextInputLayout
    private lateinit var fab: FloatingActionButton

    private lateinit var executor: SchemaExecutor

    private val ROW_ID = "RowId"

    private fun observeQueryResults() {
        lifecycleScope.launch {
            executor.state.collect {
                val record = it.values
                val res = record.map {
                    it.toTypedArray()
                }.toTypedArray()
                updateTableView(res, ROW_ID, *it.fields.toTypedArray())
            }
        }
    }

    private fun observeExecuteButton() {
        textInputLayout.setEndIconOnClickListener {
            executor.execute(queryEditText.text.toString())
        }
    }

    private fun observeFabClicked() {
        fab.setOnClickListener {
            showTableListSheet()
        }
    }

    private fun updateTableView(arr: Array<Array<String>>, vararg headers: String) {
        tableView.headerAdapter = SimpleTableHeaderAdapter(this, *headers)
        tableView.dataAdapter = SimpleTableDataAdapter(this, arr)
    }

    private fun showTableListSheet() {
        TableListBottomSheet({
            executor.getTables().map {
                TableUIModel(it)
            }
        }) {
            executor.getTableInfo(it)
        }.also {
            it.show(supportFragmentManager, TableListBottomSheet.TAG)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        SchemasServiceLocator.ctx = this
        executor = SchemaExecutor(this)

        tableView = findViewById(R.id.queryResultsTable)
        textInputLayout = findViewById(R.id.queryInputLayout)
        queryEditText = findViewById(R.id.queryInputEditText)

        fab = findViewById(R.id.fab)

        observeQueryResults()
        observeExecuteButton()
        observeFabClicked()

//        testCreate()


//
//        testSchema2.put("email", "testEmail2")
//        testSchema2.put("password", "testPassword2")
//
//        val testBuf = testSchema.toByteBuffer()
//        val testBuf2 = testSchema2.toByteBuffer()
//
//        val row =
//            com.example.saa_project_db_engine.db.storage.models.TableRow(testBuf)
//
////        val row2 =
////            com.example.saa_project_db_engine.db.storage.models.TableRow(6, testBuf2)
////
////        val bytes = com.example.saa_project_db_engine.db.storage.models.HeapPageData(
////            1,
////            2,
////            3,
////            mutableListOf(row, row2)
////        ).toBytes()
//
////        val decode =
////            com.example.saa_project_db_engine.db.storage.models.HeapPageData.fromBytes(bytes)
        // //
////        decode.records.forEach {
////            Log.d("TESTRECORD", "${it.value}")
////            testSchema3.load(it.value as ByteBuffer)
////            Log.d("TESTRECORD", "${testSchema3.get("email")} ${testSchema3.get("password")}")
////        }
//
////        Log.d("TEST", "ARRAY: ${decode.records}")
////
////        Log.d("TEST", "decoded: ${decode.id} ${decode.previousPageId} ${decode.nextPageId}")
//
//
//        val file = File(filesDir, "test.db")
        //
//        val fileManager = HeapFileManager.new(file)
//        val pageManager = HeapPageManager(fileManager)
//
//        val schema = Schema.Parser().parse(schemaFile)
//
//        val record = GenericRecord(schema)
//
//        pageManager.insertRow(row)
//
//        val page = pageManager.get(ROOT_PAGE_ID)
//
//        page!!.records.forEach {
//            record.load(it.value)
//            val email = record.get("email")
//            val pass = record.get("password")
////            Log.d("TEST", "EMAIL: $email")
////            Log.d("TEST", "PASS: $pass")
//        }
//
//        val row2 =
//            com.example.saa_project_db_engine.db.storage.models.TableRow(testBuf)
//
//        pageManager.insertRow(row2)
//
//        val page2 = pageManager.get(ROOT_PAGE_ID)
//
//        page2!!.records.forEach {
//            Log.d("TEST", "row id is ${it.rowId}")
//            record.load(it.value)
//            val email = record.get("email")
//            val pass = record.get("password")
//            Log.d("TEST", "EMAIL: $email")
//            Log.d("TEST", "PASS: $pass")
//        }

    }

    private fun testCreate() {
//        val parser = StatementParser()
//        val query =
//            parser.parseQuery(
//                "Select DISTINCT Name FROM Sample WHERE (Name == 'PETKAN' AND Name == 'IVAN') ORDER BY Id"
//            )
//        Log.d(
//            "TEST", "QUERY: $query"
//        )

        executor.execute("CreateTable Sample(Id:int, Name:string, BirthDate:string default '01.01.2022')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (4, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (6, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (3, 'DRAGAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (1, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (5, 'PETKAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (2, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (7, 'DRAGAN')")

        executor.execute("CreateIndex SampleId ON Sample (Name)")

//        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' OR Name == 'PETKAN'")

//        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' ORDER BY Id")

//        executor.execute("Delete FROM Sample WHERE Name == 'IVAN'")

//        executor.execute("Select Id FROM Sample WHERE NOT Name == 'IVAN'")

//        executor.execute("TableInfo Sample")

//        executor.execute("DropIndex Sample SampleId")

//        executor.execute("ListTables")
//
//        executor.execute("ListTables")
//
//        executor.execute("DropTable Sample")

        // Select BirthDate FROM Sample WHERE Name == 'IVAN' AND Id > '2'

        // "Select Id FROM Sample WHERE (Name == 'IVAN' AND Id == '2') OR Name == 'PETKAN'"

//        executor.execute("Select Name, BirthDate FROM Sample WHERE (Id == '1' OR BirthDate == '01.01.2022') AND Id >= '1'")


        // Select Name, DateBirth FROM Sample WHERE (Id == '6' OR Id == '6') AND (Id == '7' OR Id == '7') OR (Id == '8' OR Id == '8')
//        val parser = StatementParser()
//        val query =
//            parser.parseQuery(
//                "Select Name FROM Sample WHERE Name == 'PETKAN' OR Id == 6 AND Id > 7"
//            )
//        Log.d(
//            "TEST", "QUERY: $query"
//        )

    }
}