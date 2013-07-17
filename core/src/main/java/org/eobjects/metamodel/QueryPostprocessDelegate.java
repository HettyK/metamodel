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
package org.eobjects.metamodel;

import org.eobjects.metamodel.schema.Schema;

/**
 * A simple subclass of {@link QueryPostprocessDataContext} which provides less
 * implementation fuzz when custom querying features (like composite
 * datacontexts or type conversion) is needed.
 * 
 * @author Kasper Sørensen
 * @author Ankit Kumar
 */
public abstract class QueryPostprocessDelegate extends
		QueryPostprocessDataContext {

	@Override
	protected String getMainSchemaName() throws MetaModelException {
		throw new UnsupportedOperationException(
				"QueryPostprocessDelegate cannot perform schema exploration");
	}

	@Override
	protected Schema getMainSchema() throws MetaModelException {
		throw new UnsupportedOperationException(
				"QueryPostprocessDelegate cannot perform schema exploration");
	}
}
