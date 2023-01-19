package com.example.saa_project_db_engine.ui.adapters

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.TextView
import androidx.recyclerview.widget.DiffUtil
import androidx.recyclerview.widget.ListAdapter
import androidx.recyclerview.widget.RecyclerView
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.ui.models.TableUIModel

class TablesAdapter(private val onClick: (TableUIModel) -> Unit) :
    ListAdapter<TableUIModel, TablesAdapter.TablesViewHolder>(TableUIModelDiffCallback) {

    class TablesViewHolder(itemView: View, val onClick: (TableUIModel) -> Unit) :
        RecyclerView.ViewHolder(itemView) {
        private val tableNameTextView: TextView = itemView.findViewById(R.id.table_name)
        private var currentTable: TableUIModel? = null

        init {
            itemView.setOnClickListener {
                currentTable?.let {
                    onClick(it)
                }
            }
        }

        fun bind(table: TableUIModel) {
            currentTable = table

            tableNameTextView.text = table.name
        }
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): TablesViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.table_list_item, parent, false)
        return TablesViewHolder(view, onClick)
    }

    override fun onBindViewHolder(holder: TablesViewHolder, position: Int) {
        val flower = getItem(position)
        holder.bind(flower)
    }
}

object TableUIModelDiffCallback : DiffUtil.ItemCallback<TableUIModel>() {
    override fun areItemsTheSame(oldItem: TableUIModel, newItem: TableUIModel): Boolean {
        return oldItem == newItem
    }

    override fun areContentsTheSame(oldItem: TableUIModel, newItem: TableUIModel): Boolean {
        return oldItem.name == newItem.name
    }
}