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
 */package org.apache.metamodel;


import junit.framework.TestCase;

import org.apache.metamodel.datahub.DatahubDataContext;
import org.apache.metamodel.schema.Schema;


public class DatahubDataContextTest extends TestCase  
{
    public void testDummy() {
        // there should be at least one test  method
    }
    
    public void xtestMDMRepoConnection() {
        String host = "mdmregtest.humaninference.com";
        Integer port = 8443;
        String username = "cdiadmin";
        String password = "cdi123";
        String tenantId = "mdmregtest";
        boolean https = false;
        
        DatahubDataContext context = new DatahubDataContext(host, port, username, password, tenantId, https);
        Schema schema = context.testGetMainSchema();
        //assertEquals(4, schema.getTableCount());
        //assertEquals(13, schema.getTableByName("CUSTOMERS").getColumnCount());
        
    }

    public void xtestMonitorDemoRepoConnection() {
        String host = "localhost";
        Integer port = 8081;
        String username = "admin";
        String password = "admin";
        String tenantId = "demo";
        boolean https = false;
        
        DatahubDataContext context = new DatahubDataContext(host, port, username, password, tenantId, https);
        Schema schema = context.testGetMainSchema();
        assertEquals(6, schema.getTableCount());
        assertEquals(13, schema.getTableByName("CUSTOMERS").getColumnCount());
        
    }

}
