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
package org.eobjects.metamodel.jdbc.dialects;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectClause;

public class SQLServerQueryRewriter extends DefaultQueryRewriter {

	public SQLServerQueryRewriter(JdbcDataContext dataContext) {
		super(dataContext);
	}

	@Override
	public boolean isMaxRowsSupported() {
		return true;
	}

	/**
	 * SQL server expects the fully qualified column name, including schema, in
	 * select items.
	 */
	@Override
	public boolean isSchemaIncludedInColumnPaths() {
		return true;
	}

	@Override
	protected String rewriteSelectClause(Query query, SelectClause selectClause) {
		String result = super.rewriteSelectClause(query, selectClause);

		Integer maxRows = query.getMaxRows();
		if (maxRows != null) {
			result = "SELECT TOP " + maxRows + " " + result.substring(7);
		}

		return result;
	}
}