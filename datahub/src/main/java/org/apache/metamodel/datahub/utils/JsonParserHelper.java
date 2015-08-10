package org.apache.metamodel.datahub.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
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
import com.fasterxml.jackson.core.JsonParser;

public class JsonParserHelper {

    public static List<String> parseArray(String jsonString)
            throws IOException, JsonParseException {
        JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createParser(jsonString);
        boolean inArray = false;
        List<String> resultList = new ArrayList<String>();
        while (parser.nextToken() != null) {
            switch (parser.getCurrentToken()) {
            case START_ARRAY:
                inArray = true;
                break;
            case END_ARRAY:
                inArray = false;
                break;
            case VALUE_STRING:
                if (inArray) {
                    String arrayItem = parser.getText();
                    resultList.add(arrayItem);
                }
                break;
            default:
                break;
            }
        }
        return resultList;
    }

}
