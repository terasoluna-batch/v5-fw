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

/**
 * Class for parsing the table display of spock.
 */

class Value {

    Object value

    Value(Object value) {
        this.value = value
    }

    Row or(Object value) {
        if (value instanceof Value) {
            return new Row(this, value)
        } else {
            return new Row(this, new Value(value))
        }
    }

}

class Row extends ArrayList<Value> {

    Row(Value... values) {
        values.each { this << it }
    }

    Row or(Object value) {
        if (value instanceof Value) {
            this << value
        } else {
            this << new Value(value)
        }
        return this
    }
}

class Rows extends ArrayList<Row> {
    Rows(Row... rows) {
        rows.each { this << it }
    }
}

class Column {
    String name

    Column(String name) {
        this.name = name
    }

    Columns or(Column column) {
        return new Columns(this, column)
    }

}

class Columns extends ArrayList<Column> {
    Columns(Column... columns) {
        columns.each { this << it }
    }

    Columns or(Column column) {
        this << column
        return this
    }
}

class Table {

    String name
    Columns columns
    Rows rows

    Table(String name) {
        this.name = name
    }
}

class Tables {
    List<Table> tables = []

    Tables() {}

    Tables(Table... tables) {
        tables.each { this.tables << it }
    }

    @Override
    Object invokeMethod(String name, Object args) {
        if (args == null) {
            throw new NullPointerException()
        }
        if (!(args instanceof Object[]) || !(args[0] instanceof Closure)) {
            throw new IllegalArgumentException()
        }

        try {
            Closure cls = args[0] as Closure
            TableParser.threadLocal.set(new Table(name))
            use(TableParser) {
                cls.delegate = new TableParser()
                cls.call()
            }
            tables << TableParser.threadLocal.get()
        } finally {
            TableParser.threadLocal.remove()
        }
    }

}

class TableParser {
    static ThreadLocal<Table> threadLocal = new ThreadLocal<>()

    static or(self, operand) {
        Row row = new Value(self) | operand

        if (threadLocal.get().rows == null) {
            threadLocal.get().rows = new Rows()
        }
        threadLocal.get().rows << row
        return row
    }

    def propertyMissing(String name) {
        if (threadLocal.get().columns == null) {
            threadLocal.get().columns = new Columns()
        }
        Column column = new Column(name)
        threadLocal.get().columns << column
        return column
    }
}
