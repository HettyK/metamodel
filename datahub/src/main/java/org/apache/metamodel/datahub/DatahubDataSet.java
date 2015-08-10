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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;

/**
 * dataset with fixed query result of 3 rows
 * 
 * @author hetty
 *
 */
public class DatahubDataSet extends AbstractDataSet {

    List<Object[]> queryResult;
    AtomicInteger _index;
    private Object[] record;

    DatahubDataSet(Column[] columns) {
        
        // TODO dummy implementation
        super(columns);
        queryResult = new ArrayList<Object[]>();
        for (int y = 0; y < 3; ++y) {
            Object[] row = new Object[columns.length];
            for (int i = 0; i < columns.length; ++i) {
                row[i] = "row" + y + ":value" + i;
            }
            queryResult.add(row);
        }
        _index = new AtomicInteger();
    }

    @Override
    public boolean next() {
        int index = _index.getAndIncrement();
        if (index < queryResult.size()) {
            record = queryResult.get(index);
            return true;
        }
        record = null;
        _index.set(0);
        return false;
    }

    @Override
    public Row getRow() {
        if (record != null) {
            final DataSetHeader header = super.getHeader();
            return new DefaultRow(header, record);
        }
        return null;

    }

}
