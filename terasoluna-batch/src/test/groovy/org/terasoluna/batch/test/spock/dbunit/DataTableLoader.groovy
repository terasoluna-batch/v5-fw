/*
 * Copyright (C) 2017 NTT DATA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.terasoluna.batch.test.spock.dbunit

import org.dbunit.dataset.IDataSet
import org.dbunit.dataset.xml.FlatXmlDataSet

/**
 * To convert the table notation defined in the closure to the DataSet of DBunit.
 *
 * <p>
 * Table display method that has been defined by the closure are as follows.
 * </p>
 *
 * <pre>
 * <code>
 * {
 *    table-name {
 *        column-name | column-name | column-name
 *        1           | 'spock'     | '1900-01-01'
 *        2           | 'dbunit'    | '1900-01-02'
 *        3           | 'test'      | '1900-01-03'
 *    }
 *    another-table-name {
 *        column-name | column-name | column-name
 *        1           | 'spock'     | '1900-01-01'
 *        2           | 'dbunit'    | '1900-01-02'
 *        3           | 'test'      | '1900-01-03'
 *    }
 * }
 * </code>
 * </pre>
 */
class DataTableLoader {

    static IDataSet loadDataSet(Closure cls) {
        cls.delegate = new Tables()
        cls.call()
        return loadDataSet(cls.tables as List<Table>)
    }

    protected static IDataSet loadDataSet(List<Table> tables) {
        def dataSetBuilder = new StringBuilder("<dataset>")

        tables.each { tbl ->
            def table = tbl
            def tableName = tbl.name

            (0..<table.rows.size()).each { row ->
                def rowBuilder = new StringBuilder(tableName)
                int rowIndex = row

                (0..<table.columns.size()).each { col ->
                    int columnIndex = col
                    def column = table.columns[columnIndex].name
                    def value = table.rows[rowIndex].value[columnIndex]

                    if (value != null) {
                        rowBuilder.append(/ ${column}="${value}"/)
                    }
                }

                dataSetBuilder.append("<${rowBuilder.toString()}/>")
            }
        }
        dataSetBuilder.append("</dataset>")

        return new FlatXmlDataSet(new StringReader(dataSetBuilder.toString()))
    }
}
