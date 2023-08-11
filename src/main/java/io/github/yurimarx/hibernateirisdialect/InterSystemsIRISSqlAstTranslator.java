/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package io.github.yurimarx.hibernateirisdialect;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.expression.*;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.ast.tree.select.SelectClause;
import org.hibernate.sql.exec.spi.JdbcOperation;

/**
 * A SQL AST translator for InterSystems IRIS.
 *
 * @author Yuri Marx Pereira Gomes
 * @author Christian Beikov
 * 
 */
public class InterSystemsIRISSqlAstTranslator <T extends JdbcOperation> extends AbstractSqlAstTranslator<T> {

	public InterSystemsIRISSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
		super( sessionFactory, statement );
	}

	@Override
	protected LockStrategy determineLockingStrategy(
			QuerySpec querySpec,
			ForUpdateClause forUpdateClause,
			Boolean followOnLocking) {
		return LockStrategy.NONE;
	}

	@Override
	protected void renderForUpdateClause(QuerySpec querySpec, ForUpdateClause forUpdateClause) {
		// IRIS does not support the FOR UPDATE clause
	}

	@Override
	protected boolean needsRowsToSkip() {
		return true;
	}

	@Override
	protected void visitSqlSelections(SelectClause selectClause) {
		renderTopClause( (QuerySpec) getQueryPartStack().getCurrent(), true, true );
		super.visitSqlSelections( selectClause );
	}

	@Override
	protected void renderTopClause(QuerySpec querySpec, boolean addOffset, boolean needsParenthesis) {
		assertRowsOnlyFetchClauseType( querySpec );
		super.renderTopClause( querySpec, addOffset, needsParenthesis );
	}

	@Override
	protected void renderFetchPlusOffsetExpression(
			Expression fetchClauseExpression,
			Expression offsetClauseExpression,
			int offset) {
		renderFetchPlusOffsetExpressionAsSingleParameter( fetchClauseExpression, offsetClauseExpression, offset );
	}

	@Override
	public void visitOffsetFetchClause(QueryPart queryPart) {
		// IRIS only supports the TOP clause
		if ( !queryPart.isRoot() && queryPart.getOffsetClauseExpression() != null ) {
			throw new IllegalArgumentException( "Can't emulate offset clause in subquery" );
		}
	}

	@Override
	protected void renderComparison(Expression lhs, ComparisonOperator operator, Expression rhs) {
		switch (operator) {
			case DISTINCT_FROM:
//				((A <> B OR A IS NULL OR B IS NULL) AND NOT (A IS NULL AND B IS NULL))
				appendSql(OPEN_PARENTHESIS);
				appendSql(OPEN_PARENTHESIS);
				lhs.accept( this );
				appendSql(" <> ");
				rhs.accept( this );
				appendSql(" OR ");
				lhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(" OR ");
				rhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(CLOSE_PARENTHESIS);
				appendSql(" AND NOT ");
				appendSql(OPEN_PARENTHESIS);
				lhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(" AND ");
				rhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(CLOSE_PARENTHESIS);
				appendSql(CLOSE_PARENTHESIS);
				break;
			case NOT_DISTINCT_FROM:
//				(NOT (A <> B OR A IS NULL OR B IS NULL) OR (A IS NULL AND B IS NULL))
				appendSql(OPEN_PARENTHESIS);
				appendSql("NOT ");
				appendSql(OPEN_PARENTHESIS);
				lhs.accept( this );
				appendSql(" <> ");
				rhs.accept( this );
				appendSql(" OR ");
				lhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(" OR ");
				rhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(CLOSE_PARENTHESIS);
				appendSql(" OR ");
				appendSql(OPEN_PARENTHESIS);
				lhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(" AND ");
				rhs.accept( this );
				appendSql(" IS NULL ");
				appendSql(CLOSE_PARENTHESIS);
				appendSql(CLOSE_PARENTHESIS);
				break;
			default:
				super.renderComparison(lhs, operator, rhs);
		}
	}

	@Override
	protected void renderSelectTupleComparison(
			List<SqlSelection> lhsExpressions,
			SqlTuple tuple,
			ComparisonOperator operator) {
		emulateSelectTupleComparison( lhsExpressions, tuple.getExpressions(), operator, true );
	}

	@Override
	protected void renderPartitionItem(Expression expression) {
		if ( expression instanceof Literal ) {
			appendSql( "'0' || '0'" );
		}
		else if ( expression instanceof Summarization ) {
			// This could theoretically be emulated by rendering all grouping variations of the query and
			// connect them via union all but that's probably pretty inefficient and would have to happen
			// on the query spec level
			throw new UnsupportedOperationException( "Summarization is not supported by DBMS!" );
		}
		else {
			expression.accept( this );
		}
	}

	@Override
	protected boolean supportsRowValueConstructorSyntax() {
		return false;
	}

	@Override
	protected boolean supportsRowValueConstructorSyntaxInInList() {
		return false;
	}

	@Override
	protected boolean supportsRowValueConstructorSyntaxInQuantifiedPredicates() {
		return false;
	}

	boolean selectDistinct = false;
	@Override
	public void visitSelectClause(SelectClause selectClause) {
		selectDistinct = selectClause.isDistinct();
		super.visitSelectClause(selectClause);
		selectDistinct = false;
	}

	List<ColumnReference> groupBy = Collections.emptyList();

	@Override
	public void visitQuerySpec(QuerySpec querySpec) {
		List<Expression> groupByAll = querySpec.getGroupByClauseExpressions();
		if (groupByAll.size() > 0) {
			groupBy = ((SqlTuple) groupByAll.get(0)).getExpressions().stream().map(el -> el.getColumnReference()).collect(Collectors.toList());
		}
		super.visitQuerySpec(querySpec);
		groupBy = Collections.emptyList();
	}

	@Override
	protected void renderSelectExpression(Expression expression) {
		if ( expression instanceof Predicate) {
			renderExpressionAsClauseItem(expression);
		}
		else if (
				expression instanceof ColumnReference && (selectDistinct || groupBy.contains(expression))
		) {
			appendSql( "%EXACT " );
			expression.accept( this );
			appendSql( " as " );
			appendSql( ((ColumnReference) expression).getSelectableName() );
		}
		else {
			expression.accept( this );
		}
	}


	@Override
	protected boolean supportsIntersect() {
		return false;
	}
}