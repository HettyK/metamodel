/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.metamodel.datahub.utils;

import org.apache.metamodel.datahub.DatahubTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ImmutableColumn;
import org.apache.metamodel.schema.Table;


public class DatahubColumnBuilder {
    String _name;
    ColumnType _type;
    Integer _number;
    Integer _size;
    String _nativeType;
    boolean _nullable;
    String _remarks;
    boolean _indexed;
    String _quote;
    boolean _primaryKey;
    Table _table;
    
    public void withName(String name) {
        _name = name;
    }
    
    Column build() {
        return new ImmutableColumn(_name,
                _type, _table, _number, _size, _nativeType, _nullable,
                _remarks, _indexed, _quote, _primaryKey);
    }

    public void withIndexed(boolean indexed) {
        _indexed = indexed;
    }

    public void withQuote(String quote) {
        _quote = quote;
        
    }

    public void withPrimaryKey(boolean primaryKey) {
        _primaryKey = primaryKey;
        
    }

    public void withRemarks(String remarks) {
        _remarks = remarks;
        
    }

    public void withNullable(boolean nullable) {
        _nullable = nullable;
        
    }

    public void withType(String type) {
        _type = toColumnType(type);
        
    }

    private ColumnType toColumnType(String columnType) {
        if (columnType.equals("INTEGER")) {
            return ColumnType.INTEGER;
        } else if (columnType.equals("LIST")) {
            return ColumnType.LIST;
        } else if (columnType.equals("BIGINT")) {
            return ColumnType.BIGINT;
        } else if (columnType.equals("VARCHAR")) {
            return ColumnType.VARCHAR;
        } else if (columnType.equals("TIMESTAMP")) {
            return ColumnType.TIMESTAMP;
        } else if (columnType.equals("DATE")) {
            return ColumnType.DATE;
        } else if (columnType.equals("BOOLEAN")) {
            return ColumnType.BOOLEAN;
        }
        //TODO throw exception?
        return null;
    }

    public void withNativeType(String nativeType) {
        _nativeType = nativeType;
        
    }

    public void withSize(Integer size) {
        _size = size;
        
    }

    public void withTable(DatahubTable table) {
        _table = table;
    }

    public void withNumber(Integer number) {
        _number = number;
        
    }

}
