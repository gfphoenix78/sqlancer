package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresInExpr implements PostgresExpression {
    @Override
    public PgType getExpressionType() {
        return globalState.getBoolType();
    }
    public PostgresInExpr(PostgresGlobalState globalState, PostgresExpression expr, PostgresExpression []expr_list) {
        assert globalState != null;
        assert expr != null;
        assert expr_list != null;
        this.globalState = globalState;
        this.expr = expr;
        this.expr_list = expr_list;
    }
    public PostgresInExpr(PostgresGlobalState globalState, PostgresExpression expr, PostgresSelect subSelect) {
        assert globalState != null;
        assert expr != null;
        assert subSelect != null;
        this.globalState = globalState;
        this.expr = expr;
        this.subSelect = subSelect;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.expr.toString()).append(" IN (");
        if (this.expr_list != null)
            sb.append(Stream.of(expr_list).map(Object::toString).collect(Collectors.joining(", ")));
        else
            sb.append(subSelect.toString());

        return sb.append(")").toString();
    }

    PostgresGlobalState globalState;
    PostgresExpression expr;
    PostgresExpression []expr_list; // expr IN (expr1, expr2, ...)
    PostgresSelect subSelect; // expr IN (subquery)
}
