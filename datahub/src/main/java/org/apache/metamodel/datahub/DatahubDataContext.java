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

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class DatahubDataContext extends QueryPostprocessDataContext implements UpdateableDataContext{

    
    public DatahubDataContext(String _host, String _port, String _username,
            String _password) {
        
        // TODO Auto-generated constructor stub
    }

    @Override
    public void executeUpdate(UpdateScript arg0) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public DataSet executeQuery(final Query query) {
        Table table = query.getFromClause().getItem(0).getTable();
        return new DatahubDataSet(table.getColumns());
        
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        return 3;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        return new DatahubSchema(getMainSchemaName());
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "Datahub";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
            int maxRows) {
        //executes a simple query and returns the result
//        final StringBuilder sb = new StringBuilder();
//        sb.append("SELECT ");
//        for (int i = 0; i < columns.length; i++) {
//            if (i != 0) {
//                sb.append(',');
//            }
//            sb.append(columns[i].getName());
//        }
//        sb.append(" FROM ");
//        sb.append(table.getName());
//
//        if (maxRows > 0) {
//            sb.append(" LIMIT " + maxRows);
//        }
//
//        final QueryResult queryResult = executeSoqlQuery(sb.toString());
        return new DatahubDataSet(columns);
    }

}
