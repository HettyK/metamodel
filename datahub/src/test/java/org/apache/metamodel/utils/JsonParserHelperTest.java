/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * \"License\"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.utils;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.metamodel.datahub.DatahubSchema;
import org.apache.metamodel.datahub.utils.JsonParserHelper;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

import com.fasterxml.jackson.core.JsonParseException;

public class JsonParserHelperTest extends TestCase {

    public void testParseJsonSchema() throws JsonParseException, IOException {

        String jsonString = "{\"schemas\":["
                + "{\"tables\":[],\"name\":\"INFORMATION_SCHEMA\"},{\"tables\":["
                + "{\"name\":\"CUSTOMERS\",\"columns\":["
                + "{\"indexed\":true,\"quote\":\"\\\"\",\"primaryKey\":true,\"name\":\"CUSTOMERNUMBER\",\"remarks\":\"\",\"nullable\":false,\"type\":\"INTEGER\",\"nativeType\":\"INTEGER\",\"size\":\"0\",\"number\": 0},"
                + "{\"indexed\":true,\"quote\":null,\"primaryKey\":false,\"name\":\"CUSTOMERNAME\",\"remarks\":null,\"nullable\":false,\"type\":\"VARCHAR\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\":1},"
                + "{\"indexed\":false,\"quote\":\"\\\"\",\"primaryKey\":false,\"name\":\"CONTACTLASTNAME\",\"remarks\":\"\",\"nullable\":false,\"type\":\"VARCHAR\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\":2},"
                + "{\"indexed\":false,\"quote\":\"\\\"\",\"primaryKey\":false,\"name\":\"CONTACTFIRSTNAME\",\"remarks\":\"\",\"nullable\":false,\"type\":\"VARCHAR\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\":3},"
                + "{\"indexed\":false,\"quote\":\"\\\"\",\"primaryKey\":false,\"name\":\"PHONE\",\"remarks\":\"\",\"nullable\":false,\"type\":\"VARCHAR\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\": 4}]},"
                + "{\"name\":\"SUPPLIERS\",\"columns\":["
                + "{\"indexed\":true,\"quote\":\"\\\"\",\"primaryKey\":true,\"name\":\"SUPPLIERNUMBER\",\"remarks\":\"\",\"nullable\":false,\"type\":\"INTEGER\",\"nativeType\":\"INTEGER\",\"size\":\"0\",\"number\":0},"
                + "{\"indexed\":false,\"quote\":\"\\\"\",\"primaryKey\":false,\"name\":\"SUPPLIERNAME\",\"remarks\":\"\",\"nullable\":false,\"type\":\"VARCHAR\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\":1},"
                + "{\"indexed\":false,\"quote\":\"\\\"\",\"primaryKey\":false,\"name\":\"ACTIVE\",\"remarks\":\"\",\"nullable\":false,\"type\":\"BOOLEAN\",\"nativeType\":\"VARCHAR\",\"size\":\"50\",\"number\":2}]}"
                + "],\"name\":\"PUBLIC\"}]}";
        JsonParserHelper parser = new JsonParserHelper();
        DatahubSchema schema = parser.parseJsonSchema(jsonString);
        assertNotNull(schema);
        assertEquals(2, schema.getTableCount());
        Table customersTable = schema.getTableByName("CUSTOMERS");
        assertNotNull(customersTable);
        assertEquals(5, customersTable.getColumnCount());
        Column customernameColumn = customersTable.getColumnByName("CUSTOMERNAME");
        assertEquals(true, customernameColumn.isIndexed());
        assertEquals(false, customernameColumn.isPrimaryKey());
        assertEquals(false, customernameColumn.isNullable().booleanValue());
        assertEquals(null, customernameColumn.getQuote());
        assertEquals(null, customernameColumn.getRemarks());
        assertEquals(customersTable, customernameColumn.getTable());
        assertEquals(ColumnType.VARCHAR, customernameColumn.getType());
        assertEquals("VARCHAR", customernameColumn.getNativeType());
        assertEquals(50, customernameColumn.getColumnSize().intValue());
        assertEquals(1, customernameColumn.getColumnNumber());
        Table suppliersTable = schema.getTableByName("SUPPLIERS");
        assertNotNull(suppliersTable);
        assertEquals(3, suppliersTable.getColumnCount());
        assertEquals(ColumnType.INTEGER,
                suppliersTable.getColumnByName("SUPPLIERNUMBER").getType());
        assertEquals(ColumnType.VARCHAR,
                suppliersTable.getColumnByName("SUPPLIERNAME").getType());
        assertEquals(ColumnType.BOOLEAN,
                suppliersTable.getColumnByName("ACTIVE").getType());

    }

    public void testParseDatastoreArray() throws IOException {
        String jsonString = "[{\"name\":\"CSV datastore for Person Golden Records\",\"description\":\"The CSV datastore containing offboarded golden records for person configuration\",\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"Enrichment Service Call Logs\",\"description\":\"The CSV data store containing enrichment service call logs\",\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"Golden records\",\"description\":\"Virtual datastore for Golden Records\",\"type\":\"GoldenRecordDatastore\"},"
                + "{\"name\":\"GoldenRecord Organization Export\",\"description\":null,\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"GoldenRecord Person Export\",\"description\":null,\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"Golden_records20150810133835556.CSV\",\"description\":null,\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"Golden_records20150810133955872.CSV\",\"description\":null,\"type\":\"CsvDatastore\"},"
                + "{\"name\":\"MDM datastore\",\"description\":\"Physical datastore of MDM\",\"type\":\"MDMDatastore\"}]";
        JsonParserHelper parser = new JsonParserHelper();
        List<String> names = parser.parseDataStoreArray(jsonString);
        assertEquals(2, names.size());
    }

}
