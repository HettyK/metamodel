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
package org.eobjects.metamodel.query;

import org.eobjects.metamodel.MetaModelTestCase;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Relationship;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class FromItemTest extends MetaModelTestCase {

	private Schema _schema = getExampleSchema();

	public void testExpressionBased() throws Exception {
		FromItem fromItem = new FromItem("foobar");
		assertEquals("foobar", fromItem.toString());
		fromItem.setAlias("f");
		assertEquals("foobar f", fromItem.toString());

		assertEquals("SELECT COUNT(*) FROM foobar", new Query().selectCount().from(
				"foobar").toString());
	}

	public void testRelationJoinToString() throws Exception {
		Table contributorTable = _schema.getTableByName(TABLE_CONTRIBUTOR);
		Table roleTable = _schema.getTableByName(TABLE_ROLE);
		Relationship[] relationships = roleTable
				.getRelationships(contributorTable);
		FromItem from = new FromItem(JoinType.INNER, relationships[0]);
		assertEquals(
				"MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id",
				from.toString());

		from.setAlias("myJoin");
		assertEquals(
				"(MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id) myJoin",
				from.toString());

		from.getLeftSide().setAlias("a");
		assertEquals(
				"(MetaModelSchema.contributor a INNER JOIN MetaModelSchema.role ON a.contributor_id = role.contributor_id) myJoin",
				from.toString());
	}

	public void testSubQueryJoinToString() throws Exception {
		Table projectTable = _schema.getTableByName(TABLE_PROJECT);
		Table roleTable = _schema.getTableByName(TABLE_ROLE);

		Column projectIdColumn = projectTable
				.getColumnByName(COLUMN_PROJECT_PROJECT_ID);

		FromItem leftSide = new FromItem(projectTable);
		leftSide.setAlias("a");
		SelectItem[] leftOn = new SelectItem[] { new SelectItem(projectIdColumn) };

		Column[] columns = roleTable.getColumns();

		Query subQuery = new Query();
		FromItem subQueryFrom = new FromItem(roleTable);
		subQuery.from(subQueryFrom);
		subQuery.select(columns);
		SelectItem subQuerySelectItem = subQuery.getSelectClause().getItems()
				.get(1);
		FromItem rightSide = new FromItem(subQuery);
		rightSide.setAlias("b");
		SelectItem[] rightOn = new SelectItem[] { subQuerySelectItem };
		FromItem from = new FromItem(JoinType.LEFT, leftSide, rightSide,
				leftOn, rightOn);

		assertEquals(
				"MetaModelSchema.project a LEFT JOIN (SELECT role.contributor_id, role.project_id, role.name FROM MetaModelSchema.role) b ON a.project_id = b.project_id",
				from.toString());

		subQueryFrom.setAlias("c");
		assertEquals(
				"MetaModelSchema.project a LEFT JOIN (SELECT c.contributor_id, c.project_id, c.name FROM MetaModelSchema.role c) b ON a.project_id = b.project_id",
				from.toString());

		subQuerySelectItem.setAlias("foobar");
		assertEquals(
				"MetaModelSchema.project a LEFT JOIN (SELECT c.contributor_id, c.project_id AS foobar, c.name FROM MetaModelSchema.role c) b ON a.project_id = b.foobar",
				from.toString());
	}
}