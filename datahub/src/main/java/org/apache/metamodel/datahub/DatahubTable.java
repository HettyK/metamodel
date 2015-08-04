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

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ImmutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;

/**
 * Dummy implementation of Datahub table, final version must be implemented in metamodel
 * @author hetty
 *
 */
public class DatahubTable extends AbstractTable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String _name;

    public DatahubTable(String name) {
        _name = name;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Column[] getColumns() {
        Column[] columns = new Column[3];
        columns[0] = new ImmutableColumn("id", ColumnType.INTEGER, this, 1, 10, "integer", false, "remarks", true, null, true);
        columns[1] = new ImmutableColumn("name", ColumnType.VARCHAR, this, 1, 50, "string", false, "remarks", false, null, false);
        columns[2] = new ImmutableColumn("age", ColumnType.INTEGER, this, 1, 10, "integer", false, "remarks", false, null, false);
        return columns;
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
