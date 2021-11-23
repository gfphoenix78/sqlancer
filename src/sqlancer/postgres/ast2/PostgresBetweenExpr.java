package sqlancer.postgres.ast2;


import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;

public final class PostgresBetweenExpr implements PostgresExpression {
    private final PostgresGlobalState globalState;
    private final PostgresExpression expr;
    private final PostgresExpression left;
    private final PostgresExpression right;
    private final boolean isSymmetric;
    private final boolean not;

    public PostgresBetweenExpr(PostgresGlobalState globalState,
                               PostgresExpression expr,
                               PostgresExpression left,
                               PostgresExpression right,
                               boolean not,
                               boolean symmetric) {
        this.globalState = globalState;
        this.expr = expr;
        this.left = left;
        this.right = right;
        this.not = not;
        this.isSymmetric = symmetric;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    public PostgresExpression getLeft() {
        return left;
    }

    public PostgresExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(expr.toString());
        if (not)
            sb.append(" NOT");
        sb.append(" BETWEEN");
        if (isSymmetric)
            sb.append(" SYMMETRIC ");
        sb.append(left.toString())
            .append(" AND ")
            .append(right.toString());
        return sb.toString();
    }

    @Override
    public PgType getExpressionType() {
        return globalState.getType("bool");
    }
}
