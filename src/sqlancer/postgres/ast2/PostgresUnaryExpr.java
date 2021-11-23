package sqlancer.postgres.ast2;

import sqlancer.postgres.PgOperators;
import sqlancer.postgres.PgType;

public class PostgresUnaryExpr implements PostgresExpression {
    // NOT
    @Override
    public PgType getExpressionType() {
        return op.getResult();
    }
    public PostgresUnaryExpr(PgOperators.PgOperator2 op, PostgresExpression expr) {
        this.op = op;
        this.expr = expr;
        if (op == null && expr != null && expr.getExpressionType().getTypname().compareTo("bool") != 0)
            throw new AssertionError("NOT expr: type of expr is not `bool`");
    }
    @Override
    public String toString() {
        return op.getName() + "(" + expr.toString() + ")";
    }

    PgOperators.PgOperator2 op; // op is null if NOT
    PostgresExpression expr;
}
