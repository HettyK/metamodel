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

import org.apache.metamodel.schema.AbstractSchema;
/**
 *  implementation of Datahub schema, final must be implemented in metamodel
 */
import org.apache.metamodel.schema.Table;

public class DatahubSchema extends AbstractSchema {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String _name;

    public DatahubSchema(String name) {
        _name = name;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Table[] getTables() {
        Table[] tables = new DatahubTable[3];
        tables[0] = new DatahubTable("table1");
        tables[1] = new DatahubTable("table2");
        tables[2] = new DatahubTable("table3");
        return tables;
    }

    @Override
    public String getQuote() {
        return null;
    }

}
