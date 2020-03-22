package sqlancer.mysql;

import sqlancer.IgnoreMeException;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLColumnValue;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperator;
import sqlancer.mysql.ast.MySQLUnaryPrefixOperation;

public class MySQLExpectedValueVisitor extends MySQLVisitor {

	private final StringBuilder sb = new StringBuilder();
	private int nrTabs = 0;

	private void print(MySQLExpression expr) {
		MySQLToStringVisitor v = new MySQLToStringVisitor();
		v.visit(expr);
		for (int i = 0; i < nrTabs; i++) {
			sb.append("\t");
		}
		sb.append(v.get());
		sb.append(" -- " + expr.getExpectedValue());
		sb.append("\n");
	}

	@Override
	public void visit(MySQLExpression expr) {
		nrTabs++;
		try {
			super.visit(expr);
		} catch (IgnoreMeException e) {

		}
		nrTabs--;
	}

	@Override
	public void visit(MySQLConstant constant) {
		print(constant);
	}

	@Override
	public void visit(MySQLColumnValue column) {
		print(column);
	}

	@Override
	public void visit(MySQLUnaryPrefixOperation op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(MySQLUnaryPostfixOperator op) {
		print(op);
		visit(op.getExpression());
	}

	@Override
	public void visit(MySQLComputableFunction f) {
		print(f);
		for (MySQLExpression expr : f.getArguments()) {
			visit(expr);
		}
	}

	@Override
	public void visit(MySQLBinaryLogicalOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	public String get() {
		return sb.toString();
	}

	@Override
	public void visit(MySQLSelect select) {
		for (MySQLJoin j : select.getJoinClauses()) {
			visit(j);
		}
		if (select.getWhereClause() != null) {
			visit(select.getWhereClause());
		}
	}

	@Override
	public void visit(MySQLBinaryComparisonOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(MySQLCastOperation op) {
		print(op);
		visit(op.getExpr());
	}

	@Override
	public void visit(MySQLInOperation op) {
		print(op);
		for (MySQLExpression right : op.getListElements()) {
			visit(right);
		}
	}

	@Override
	public void visit(MySQLBinaryOperation op) {
		print(op);
		visit(op.getLeft());
		visit(op.getRight());
	}

	@Override
	public void visit(MySQLOrderByTerm op) {
	}

	@Override
	public void visit(MySQLExists op) {
		print(op);
		visit(op.getExpr());
	}

	@Override
	public void visit(MySQLStringExpression op) {
		print(op);
	}

	@Override
	public void visit(MySQLBetweenOperation op) {
		print(op);
		visit(op.getExpr());
		visit(op.getLeft());
		visit(op.getRight());
	}

}