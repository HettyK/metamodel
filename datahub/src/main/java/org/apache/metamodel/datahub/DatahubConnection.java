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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;



/**
 * Describes the connection information needed to connect to the DataHub.
 */

public class DatahubConnection {


    private final String _hostname;
    private final int _port;
    private final String _contextPath = "DataCleaner-monitor";
    private final boolean _https = false;
    private final String _tenantId;
    private final String _username;
    private final String _password;
    private final String _datahubContext = "datastores/orderdb";
    //private final UserPreferences _userPreferences;


    public DatahubConnection(String hostname, Integer port, String username,
            String password, String tenantId) {
        _hostname = hostname;
        _port = port;
        _tenantId = tenantId;
        _username = username;
        _password = password;
        // TODO Auto-generated constructor stub
    }
    
    public HttpClient getHttpClient() {
        CloseableHttpClient httpClient  = HttpClientBuilder.create().build();
        return httpClient;
    }


    public String getHostname() {
        return _hostname;
    }


    public String getContextPath() {
        return _contextPath;
    }

    public boolean isHttps() {
        return _https;
    }

    public int getPort() {
        return _port;
    }


    public String getTenantId() {
        return _tenantId;
    }


    public String getUsername() {
        return _username;
    }


    public String getPassword() {
        return _password;
    }

    public String getRepositoryUrl() {
        return getBaseUrl() + "/repository" + (StringUtils.isEmpty(_tenantId) ? "" : "/" + _tenantId);
    }

    public String getBaseUrl() {
        StringBuilder sb = new StringBuilder();
        if (_https) {
            sb.append("https://");
        } else {
            sb.append("http://");
        }
        sb.append(_hostname);

        if ((_https && _port != 443) || (!_https && _port != 80)) {
            // only add port if it differs from default ports of HTTP/HTTPS.
            sb.append(':');
            sb.append(_port);
        }

        if (!StringUtils.isEmpty(_contextPath)) {
            sb.append('/');
            sb.append(_contextPath);
        }

        return sb.toString();
    }

    String getDatahubUrl() {
        String url = getRepositoryUrl() + "/"
        + getDatahubContextPath(); 
        return url;

    }
    public HttpHost getHttpHost() {
        String _scheme = _https ? "https" : "http" ;
        return new HttpHost(_hostname, _port, _scheme);  
    }

    public String getDatahubContextPath() {
        return _datahubContext;
    }

}
