/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.datahub;

import java.util.List;

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ImmutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;

/**
 * Dummy implementation of Datahub table, final version must be implemented in
 * metamodel
 * 
 * @author hetty
 *
 */
public class DatahubTable extends AbstractTable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String _name;
    private Column[] _columns;

    public DatahubTable(String name, List<String> columnNames) {
        _name = name;
        _columns = createColumns(columnNames);
    }

    private Column[] createColumns(List<String> columnNames) {
        Column[] columns = new Column[columnNames.size()];
        for (int i = 0; i < columnNames.size(); ++i) {

            // name the name of the column
            // type the type of the column
            // table the table which the constructed column will pertain to
            // columnNumber the column number of the column
            // columnSize the size of the column
            // nativeType the native type of the column
            // nullable whether the column's values are nullable
            // remarks the remarks of the column
            // indexed whether the column is indexed or not
            // quote the quote character(s) of the column
            // primaryKey whether the column is a primary key or not
            columns[i] = new ImmutableColumn(columnNames.get(i),
                    ColumnType.VARCHAR, this, i + 1, 10, "string", true,
                    "remarks", false, null, false);
        }

        return columns;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Column[] getColumns() {
        return _columns;
    }

    @Override
    public Schema getSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TableType getType() {
        return TableType.TABLE;
    }

    @Override
    public Relationship[] getRelationships() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRemarks() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getQuote() {
        // TODO Auto-generated method stub
        return null;
    }

}
