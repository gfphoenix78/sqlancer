package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;
import sqlancer.postgres.ast.PostgresExpression;

public class PostgresCollate implements PostgresExpression {
    private final sqlancer.postgres.ast.PostgresExpression expr;
    private final String collate;

    public PostgresCollate(PostgresExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    @Override
    public String toString() {
        return expr.toString() + " " + collate;
    }

    @Override
    public PgType getExpressionType() {
        return expr.getExpressionType();
    }

}
