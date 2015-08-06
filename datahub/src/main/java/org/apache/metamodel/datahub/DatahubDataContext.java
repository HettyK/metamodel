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
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatahubDataContext extends QueryPostprocessDataContext implements
        UpdateableDataContext {
    private static final Logger logger = LoggerFactory.getLogger(DatahubDataContext.class);


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
        return new DatahubDataSet(table.getColumns());

    }

    @Override
    protected Number executeCountQuery(Table table,
            List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        return 3;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        HttpClient client = _connection.getHttpClient();
        String uri = _connection.getDatahubUrl() + "/" + getMainSchemaName() + ".tables";
        logger.debug("request {}", uri);
        HttpGet request = new HttpGet(uri);
        request.addHeader("Accept", "application/json");
        request.addHeader("Accept-Charset", "ISO-8859-1,utf-8;q=0.7,*;q=0.7");
        try {
            //HttpResponse response = executeRequest(client, request);
            //String result = EntityUtils.toString(response.getEntity());
            //logger.debug(result);
            //JsonFactory factory = new JsonFactory();
            //JsonParser parser = factory.createParser(result);
            
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        // TODO parse the response and create a schema
        return new DatahubSchema(getMainSchemaName());
    }

    private HttpResponse executeRequest(HttpClient client, HttpGet request)
            throws IOException, ClientProtocolException {
        HttpResponse response = client.execute(
                _connection.getHttpHost(), request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 403) {
            logger.error("Access is denied");
            logger.error("Response received as " + response);
            throw new AccessControlException(
                    "You are not authorized to access the service");
        }
        if (statusCode != 200) {
            logger.error(
                    "Unexpected response status code: {}, response entity:\n{}",
                    statusCode, EntityUtils.toString(response.getEntity()));
            throw new IllegalStateException(
                    "Unexpected response status code: " + statusCode);
        }
        return response;
    }

    // http://localhost:8665/DataCleaner-monitor/repository/demo/datastores/Golden record.schemas

    @Override
    protected String getMainSchemaName() throws MetaModelException {
//        HttpClient client = _connection.getHttpClient();
//        String uri = _connection.getDatahubUrl() + ".schemas";
//        HttpGet request = new HttpGet(uri);
//        request.addHeader("Accept", "application/json");
//        request.addHeader("Accept-Charset", "ISO-8859-1,utf-8;q=0.7,*;q=0.7");
//        try {
//            HttpResponse response = executeRequest(client, request);
//        } catch (Exception e) {
//            throw new IllegalStateException(e);
//        }
//        return new DatahubSchema(getMainSchemaName());
        return "PUBLIC";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
            int maxRows) {
        // executes a simple query and returns the result
        // final StringBuilder sb = new StringBuilder();
        // sb.append("SELECT ");
        // for (int i = 0; i < columns.length; i++) {
        // if (i != 0) {
        // sb.append(',');
        // }
        // sb.append(columns[i].getName());
        // }
        // sb.append(" FROM ");
        // sb.append(table.getName());
        //
        // if (maxRows > 0) {
        // sb.append(" LIMIT " + maxRows);
        // }
        //
        // final QueryResult queryResult = executeSoqlQuery(sb.toString());
        return new DatahubDataSet(columns);
    }

}
