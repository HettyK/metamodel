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

import java.io.IOException;

import org.apache.metamodel.datahub.DatahubSchema;
import org.apache.metamodel.datahub.DatahubTable;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonParserHelper {

    private static enum DatastoreObject {
        DATASTORE {
            @Override
            public DatastoreObject previous() {
                return null; // see below for options for this line
            }
        },
        SCHEMA, TABLE, COLUMN {
            @Override
            public DatastoreObject next() {
                return null; // see below for options for this line
            };
        };
        public DatastoreObject next() {
            // No bounds checking required here, because the last instance
            // overrides
            return values()[ordinal() + 1];
        }

        public DatastoreObject previous() {
            // No bounds checking required here, because the first instance
            // overrides
            return values()[ordinal() - 1];

        }
    }

    private DatastoreObject _currentObject;
    private String _currentFieldname;
    private DatahubTable _currentTable;
    private DatahubColumnBuilder _currentColumnBuilder;
    private String _schemaName;
    private DatahubSchema _resultSchema;

    public JsonParserHelper() {

    }

    public DatahubSchema parseJsonSchema(String result, String schemaName)
            throws JsonParseException, IOException {
        _currentObject = DatastoreObject.DATASTORE;
        _currentFieldname = "";
        _schemaName = schemaName;
        _resultSchema = new DatahubSchema();
        JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createParser(result);
        JsonToken token = parser.nextToken();
        while (token != null) {
            switch (parser.getCurrentToken()) {
            case START_ARRAY:
                _currentObject = _currentObject.next();
                break;
            case END_ARRAY:
                _currentObject = _currentObject.previous();
                break;
            case START_OBJECT:
                createNewObject();
                break;
            case END_OBJECT:
                addObjectToSchema();
                break;
            case FIELD_NAME:
                _currentFieldname = parser.getText();
                break;
            case VALUE_STRING:
                handleValue(_currentFieldname, parser.getText());
                break;
            case VALUE_FALSE:
                handleBooleanValue(_currentFieldname, false);
                break;
            case VALUE_TRUE:
                handleBooleanValue(_currentFieldname, true);
                break;
            case VALUE_NUMBER_INT:
                handleIntegerValue(_currentFieldname, parser.getIntValue());
            default:
                break;
            }
            token = parser.nextToken();
        }

        return _resultSchema;

    }

    private void addObjectToSchema() {
        switch (_currentObject) {
        case TABLE:
            _currentTable.setSchema(_resultSchema);
            _resultSchema.addTable(_currentTable);
            break;
        case COLUMN:
            _currentColumnBuilder.withTable(_currentTable);
            _currentTable.add(_currentColumnBuilder.build());
            break;
        default:
        }
    }

    private void createNewObject() {
        switch (_currentObject) {
        case TABLE:
            _currentTable = new DatahubTable();
            break;
        case COLUMN:
            _currentColumnBuilder = new DatahubColumnBuilder();
            break;
        default:
        }
    }

    private void handleValue(String fieldName, String fieldValue) {
        switch (_currentObject) {
        case SCHEMA:
            handleSchemaField(fieldName, fieldValue);
            break;
        case TABLE:
            handleTableField(fieldName, fieldValue);
            break;
        case COLUMN:
            handleColumnField(fieldName, fieldValue);
            break;
        default:
        }
    }

    private void handleBooleanValue(String fieldName, boolean fieldValue) {
        switch (_currentObject) {
        case COLUMN:
            handleBooleanColumnField(fieldName, fieldValue);
            break;
        default:
        }
    }

    private void handleBooleanColumnField(String fieldName, boolean fieldValue) {
        if (fieldName.equals("primaryKey")) {
            _currentColumnBuilder.withPrimaryKey(fieldValue);
        } else if (fieldName.equals("indexed")) {
            _currentColumnBuilder.withIndexed(fieldValue);
        } else if (fieldName.equals("nullable")) {
            _currentColumnBuilder.withNullable(fieldValue);
        } else {
            // skip unknown column fields
        }

    }

    private void handleIntegerValue(String fieldName, int fieldValue) {
        if (fieldName.equals("number")) {
            _currentColumnBuilder.withNumber(fieldValue);
        }

    }

    private void handleColumnField(String fieldName, String fieldValue) {
        if (fieldName.equals("name")) {
            _currentColumnBuilder.withName(fieldValue);
        } else if (fieldName.equals("quote")) {
            _currentColumnBuilder.withQuote(fieldValue);
        } else if (fieldName.equals("remarks")) {
            _currentColumnBuilder.withRemarks(fieldValue);
        } else if (fieldName.equals("type")) {
            _currentColumnBuilder.withType(fieldValue);
        } else if (fieldName.equals("nativeType")) {
            _currentColumnBuilder.withNativeType(fieldValue);
        } else if (fieldName.equals("size")) {
            _currentColumnBuilder.withSize(new Integer(fieldValue));
        } else {
            // skip unknown column fields
        }
    }

    private void handleTableField(String fieldName, String fieldValue) {
        if (fieldName.equals("name")) {
            _currentTable.setName(fieldValue);
        }

    }

    private void handleSchemaField(String fieldName, String fieldValue) {
        if (fieldName.equals("name") && fieldValue.equals(_schemaName)) {
            _resultSchema.setName(fieldValue);
        }
    }
}
