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

import java.io.IOException;
import java.security.AccessControlException;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.datahub.utils.JsonParserHelper;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;

public class DatahubDataContext extends QueryPostprocessDataContext implements
        UpdateableDataContext {
    private static final Logger logger = LoggerFactory
            .getLogger(DatahubDataContext.class);

    private DatahubConnection _connection;

    public DatahubDataContext(String host, Integer port, String username,
            String password, String tenantId) {
        _connection = new DatahubConnection(host, port, username, password,
                tenantId);

    }

    @Override
    public void executeUpdate(UpdateScript arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public DataSet executeQuery(final Query query) {
        Table table = query.getFromClause().getItem(0).getTable();
        //TODO dummy implementation
        return new DatahubDataSet(table.getColumns());

    }

    @Override
    protected Number executeCountQuery(Table table,
            List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        //TODO dummy implementation
        return 3;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        String schemaName = getMainSchemaName();
        String uri = _connection.getDatahubUri() + "/" + schemaName
                + ".tables";
        HttpGet request = new HttpGet(uri);
        try {
            Table[] tables = getTables(schemaName, request);
            DatahubSchema schema = new DatahubSchema(schemaName, tables);
            return schema;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Table[] getTables(String schemaName, HttpGet request)
            throws IOException, ClientProtocolException, JsonParseException {
        HttpResponse response = executeRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        List<String> tableNames = JsonParserHelper.parseArray(result);
        Table[] tables = new DatahubTable[tableNames.size()];
        for (int i = 0 ; i < tableNames.size() ; ++i) {
            List<String> columnNames = getColumnNames(schemaName, tableNames.get(i));
            tables[i] = new DatahubTable(tableNames.get(i), columnNames);
        }
        return tables;
    }

    private List<String> getColumnNames(String schemaName, String tableName) {
        String uri = _connection.getDatahubUri() + "/" + schemaName + "/" + tableName + ".columns";
        logger.debug("request {}", uri);
        HttpGet request = new HttpGet(uri);
        try {
            HttpResponse response = executeRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            List<String> columnNames = JsonParserHelper.parseArray(result);
            return columnNames;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Schema testGetMainSchema() {
        return getMainSchema();
    }

    private HttpResponse executeRequest(HttpGet request)
            throws IOException, ClientProtocolException {

        HttpClient httpClient = _connection.getHttpClient();
        HttpResponse response = httpClient.execute(request, _connection.getContext());

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 403) {
            throw new AccessControlException(
                    "You are not authorized to access the service");
        }
        if (statusCode == 404) {
            throw new AccessControlException(
                    "Could not connect to Datahub: not found");
        }
        if (statusCode != 200) {
            throw new IllegalStateException("Unexpected response status code: "
                    + statusCode);
        }
        return response;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        String uri = _connection.getDatahubUri() + ".schemas";
        logger.debug("request {}", uri);
        HttpGet request = new HttpGet(uri);
        try {
            HttpResponse response = executeRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            List<String> schemas = JsonParserHelper.parseArray(result);
            if (schemas.size() > 1) {
                return schemas.get(1);
            } else {
                // expecting numbers to be at least 2
                return "";
            }

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
            int maxRows) {
        return new DatahubDataSet(columns);
    }

}
