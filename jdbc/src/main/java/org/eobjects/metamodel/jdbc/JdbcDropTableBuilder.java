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
package org.eobjects.metamodel.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eobjects.metamodel.drop.AbstractTableDropBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.jdbc.dialects.IQueryRewriter;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * {@link TableDropBuilder} that issues an SQL DROP TABLE statement
 * 
 * @author Kasper Sørensen
 */
final class JdbcDropTableBuilder extends AbstractTableDropBuilder implements TableDropBuilder {

    private final JdbcUpdateCallback _updateCallback;
    private final IQueryRewriter _queryRewriter;

    public JdbcDropTableBuilder(JdbcUpdateCallback updateCallback, Table table, IQueryRewriter queryRewriter) {
        super(table);
        _updateCallback = updateCallback;
        _queryRewriter = queryRewriter;
    }

    @Override
    public void execute() {
        String sql = createSqlStatement();

        PreparedStatement statement = _updateCallback.getPreparedStatement(sql, false);
        try {
            _updateCallback.executePreparedStatement(statement, false);

            // remove the table reference from the schema
            final Schema schema = getTable().getSchema();
            if (schema instanceof JdbcSchema) {
                ((JdbcSchema) schema).refreshTables();
            }
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute drop table statement: " + sql);
        }
    }

    protected String createSqlStatement() {
        FromItem fromItem = new FromItem(getTable());
        String tableLabel = _queryRewriter.rewriteFromItem(fromItem);

        return "DROP TABLE " + tableLabel;
    }

}
