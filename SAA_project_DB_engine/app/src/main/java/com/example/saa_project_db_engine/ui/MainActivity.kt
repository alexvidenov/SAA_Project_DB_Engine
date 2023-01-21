package com.example.saa_project_db_engine.ui

import android.os.Bundle
import android.widget.EditText
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.executor.SchemaExecutor
import com.example.saa_project_db_engine.services.SchemasServiceLocator
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

        testQueries()

    }

    private fun observeQueryResults() {
        lifecycleScope.launch {
            executor.state.collect { resModel ->
                val record = resModel.values
                val mapped = record.map {
                    it.toTypedArray()
                }.toTypedArray()
                updateTableView(mapped, *resModel.fields.toTypedArray())
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

    private fun testQueries() {

//        executor.execute("CreateTable Sample(Id:int, Name:string, BirthDate:string default '01.01.2022')")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (4, 'IVAN')")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (6, 'IVAN')")
//
//        executor.execute("Insert INTO Sample (Id, Name, BirthDate) VALUES (3, 'DRAGAN', '01.02.2022')")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (1, 'IVAN')")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (5, 'PETKAN')")
//
//        executor.execute("CreateIndex SampleId ON Sample (Name)")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (2, 'IVAN')")
//
//        executor.execute("Insert INTO Sample (Id, Name) VALUES (7, 'DRAGAN')")

        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' ORDER BY Name, Id")

//        executor.execute("Select Id, Name FROM Sample WHERE (NOT Name == 'IVAN') AND (NOT Name == 'PETKAN') AND Id > '5'")

//        executor.execute("Update Sample SET Name = 'bruh' WHERE Name == 'IVAN'")

//        executor.execute("Select Id FROM Sample WHERE (NOT Name == 'IVAN' AND Id == '3')")

//        executor.execute("Select Id FROM Sample WHERE Name == 'bruh'")

//        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' OR Name == 'PETKAN'")

//        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' ORDER BY Id")

//        executor.execute("Select DISTINCT Name, Id FROM Sample WHERE BirthDate == '01.01.2022' ORDER BY Id")
//
//        executor.execute("Select DISTINCT Name, Id FROM Sample WHERE BirthDate == '01.01.2022' ORDER BY Id")

//        executor.execute("Delete FROM Sample WHERE Name == 'IVAN'")

//        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN'")

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

    }
}