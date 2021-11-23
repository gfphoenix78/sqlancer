package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;

public class PostgresCastExpr implements PostgresExpression {
    @Override
    public PgType getExpressionType() {
        return targetType;
    }

    public PgType getTargetType() {
        return targetType;
    }

    public PostgresExpression getExpr() {
        return expr;
    }
    PostgresCastExpr(PostgresExpression expr, PgType targetType) {
        this.expr = expr;
        this.targetType = targetType;
    }

    @Override
    public String toString() {
        return "CAST(" + expr.toString() + " AS " + targetType.getTypname() + ")";
    }

    PostgresExpression expr;
    PgType targetType;
}
