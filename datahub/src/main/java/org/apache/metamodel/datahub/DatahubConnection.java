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
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;



/**
 * Describes the connection information needed to connect to the DataHub.
 */

public class DatahubConnection {


    private final String _hostname;
    private final int _port;
    private final boolean _https;
    private final String _tenantId;
    private final String _username;
    private final String _password;
    // TODO changes these to the MDM context: "ui" and "/datastores/Golden record"
    private final String _contextPath = "/DataCleaner-monitor";
    private final String _datahubContext = "/datastores/orderdb";
    private final String _scheme;
    HttpClientContext _context;


    public DatahubConnection(String hostname, Integer port, String username,
            String password, String tenantId) {
        _hostname = hostname;
        _port = port;
        _https = false;
        _tenantId = tenantId;
        _username = username;
        _password = password;
        if (_https) {
            _scheme = "https";
        } else {
            _scheme = "http";
        }

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
                AuthScope.ANY_PORT), new UsernamePasswordCredentials(getUsername(),
                getPassword()));
        _context = HttpClientContext.create();
        _context.setCredentialsProvider(credsProvider);
    }
    
    public HttpClient getHttpClient() {
        CloseableHttpClient httpClient  = HttpClients.createDefault();
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
        sb.append(_scheme).append("://" + _hostname);

        if ((_https && _port != 443) || (!_https && _port != 80)) {
            // only add port if it differs from default ports of HTTP/HTTPS.
            sb.append(':');
            sb.append(_port);
        }

        if (!StringUtils.isEmpty(_contextPath)) {
            sb.append(_contextPath);
        }

        return sb.toString();
    }

    String getDatahubUri() {
        
        String uri = getRepositoryUrl() + getDatahubContextPath(); 
        return uri;

    }
    public HttpHost getHttpHost() {
        return new HttpHost(_hostname, _port, _scheme);  
    }

    public String getDatahubContextPath() {
        return _datahubContext;
    }

    public HttpContext getContext() {
        return _context;
    }

}
