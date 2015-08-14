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

public class DatahubDataContext extends QueryPostprocessDataContext implements
        UpdateableDataContext {
    private static final Logger logger = LoggerFactory
            .getLogger(DatahubDataContext.class);

    private DatahubConnection _connection;
    
    private DatahubSchema _schema;

    public DatahubDataContext(String host, Integer port, String username,
            String password, String tenantId, boolean https) {
        _connection = new DatahubConnection(host, port, username, password,
                tenantId, https);
        _schema = getDatahubSchema();

    }

    private DatahubSchema getDatahubSchema() {
        List<String> datastoreNames = getDataStoreNames();
        _schema = new DatahubSchema();
        for (String datastoreName : datastoreNames) {
            // String schemaName = getMainSchemaName();
            String uri = _connection.getRepositoryUrl() + "/datastores" + "/"
                    + datastoreName + ".schemas";
            logger.debug("request {}", uri);
            HttpGet request = new HttpGet(uri);
            try {
                HttpResponse response = executeRequest(request);
                String result = EntityUtils.toString(response.getEntity());
                JsonParserHelper parser = new JsonParserHelper();
                DatahubSchema schema = parser.parseJsonSchema(result);
                _schema.addTables(schema.getTables());

            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return _schema;
    }

    @Override
    public void executeUpdate(UpdateScript arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public DataSet executeQuery(final Query query) {
        Table table = query.getFromClause().getItem(0).getTable();
        // TODO dummy implementation
        return new DatahubDataSet(table.getColumns());

    }

    @Override
    protected Number executeCountQuery(Table table,
            List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        // TODO dummy implementation
        return 3;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        return _schema;
    }

    private List<String> getDataStoreNames() {
        String uri = _connection.getRepositoryUrl() + "/datastores";
        logger.debug("request {}", uri);
        HttpGet request = new HttpGet(uri);
        try {
            HttpResponse response = executeRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            JsonParserHelper parser = new JsonParserHelper();
            List<String> datastoreNames = parser.parseDataStoreArray(result);
            return datastoreNames;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Schema testGetMainSchema() {
        return getMainSchema();
    }

    private HttpResponse executeRequest(HttpGet request) throws IOException,
            ClientProtocolException {

        HttpClient httpClient = _connection.getHttpClient();
        HttpResponse response = httpClient.execute(request,
                _connection.getContext());

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
        return "MDM";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
            int maxRows) {
            
            String query = createQuery(table, columns, maxRows);
            return new DatahubDataSet(columns);
//            String uri = _connection.getRepositoryUrl() + "/datastores" + "/"
//                    + datastoreName + ".schemas";
//            logger.debug("request {}", uri);
//            HttpGet request = new HttpGet(uri);
//            try {
//                HttpResponse response = executeRequest(request);
//                String result = EntityUtils.toString(response.getEntity());
//                JsonParserHelper parser = new JsonParserHelper();
//                DatahubSchema schema = parser.parseJsonSchema(result);
//                uberSchema.addTables(schema.getTables());
//
//            } catch (Exception e) {
//                throw new IllegalStateException(e);
//            }
            

    }

    private String createQuery(Table table, Column[] columns, int maxRows) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(columns[i].getName());
        }
        sb.append(" FROM ");
        sb.append(table.getName());

        if (maxRows > 0) {
            sb.append(" LIMIT " + maxRows);
        }
        return sb.toString();
    }
}