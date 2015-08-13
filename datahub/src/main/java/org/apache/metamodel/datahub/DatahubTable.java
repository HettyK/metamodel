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

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
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
    private List<Column> _columns;
    private DatahubSchema _schema;

    public DatahubTable() {
        _name = "";
        _columns = new ArrayList<Column>();
    }


    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Column[] getColumns() {
        return _columns.toArray(new Column[_columns.size()]);
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    @Override
    public TableType getType() {
        return TableType.TABLE;
    }

    @Override
    public Relationship[] getRelationships() {
        return new Relationship[0];
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public String getQuote() {
        return null;
    }

    public void setName(String name) {
        _name = name;        
    }

    public void add(Column column) {
        _columns.add(column);
    }


    public void setSchema(DatahubSchema schema) {
        _schema = schema;
        
    }

}
