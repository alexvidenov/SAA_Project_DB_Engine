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

        // simple CreateTable with default
        executor.execute("CreateTable Sample(Id:int, Name:string, BirthDate:string default '01.01.2022')")

        // Insert with multiple fields and multiple values
        executor.execute("Insert INTO Sample (Id, Name) VALUES (4, 'IVAN'), (6, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name, BirthDate) VALUES (3, 'DRAGAN', '01.02.2022')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (1, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (5, 'PETKAN')")

        // CreateIndex
        executor.execute("CreateIndex SampleName ON Sample (Name)")

        // Insert after index creation, should be consistent
        executor.execute("Insert INTO Sample (Id, Name) VALUES (2, 'IVAN')")

        executor.execute("Insert INTO Sample (Id, Name) VALUES (7, 'DRAGAN')")

        // Simple single condition select
        executor.execute("Select Name FROM Sample WHERE Id >= '3'")

        // Complex multiple logical operations select
        executor.execute("Select Id FROM Sample WHERE (NOT Name == 'IVAN' AND BirthDate == '01.01.2022') AND Name != 'PETKAN'")

        // Complex multiple subexpressions select
        executor.execute("Select Id, Name FROM Sample WHERE (NOT Name == 'IVAN') AND (NOT Name == 'PETKAN') AND Id > '5' AND BirthDate != '01.02.2022'")

        // Select with ORDER BY with multiple fields
        executor.execute("Select Id FROM Sample WHERE Name == 'IVAN' ORDER BY Name, Id")

        // Select with DISTINCT (single field)
        executor.execute("Select DISTINCT Name FROM Sample WHERE Id > '3'")

        // Select with DISTINCT (multiple fields) and ORDER BY
        executor.execute("Select DISTINCT Name, Id FROM Sample WHERE BirthDate == '01.01.2022' ORDER BY Id")

        // Update
        executor.execute("Update Sample SET Name = 'bruh' WHERE Name == 'IVAN'")

        // Check consistency after update
        executor.execute("Select Id FROM Sample WHERE Name == 'bruh'")

        // Delete
        executor.execute("Delete FROM Sample WHERE Name == 'bruh'")

        // Check consistency after delete
        executor.execute("Select Id FROM Sample WHERE Name == 'bruh'")

        // Auxiliary functions

        executor.execute("TableInfo Sample")

        executor.execute("DropIndex Sample SampleId")

        executor.execute("ListTables")

        executor.execute("DropTable Sample")
    }
}