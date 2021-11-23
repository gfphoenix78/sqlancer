package sqlancer.postgres.ast2;

import sqlancer.postgres.PgOperators;
import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;

public class PostgresBinaryExpr implements PostgresExpression {
    // AND, OR
    @Override
    public PgType getExpressionType() {
        if (op == PgOperators.AND || op == PgOperators.OR)
            return globalState.getType("bool");
        return op.getResult();
    }
    PostgresBinaryExpr(PostgresGlobalState globalState, PgOperators.PgOperator2 op, PostgresExpression left, PostgresExpression right) {
        this.globalState = globalState;
        this.op = op;
        this.left = left;
        this.right = right;
        if (op == PgOperators.AND || op == PgOperators.OR) {
            if (left != null && left.getExpressionType().getTypname().compareTo("bool") != 0)
                throw new AssertionError("AND/OR requires bool expression");
            if (right != null && right.getExpressionType().getTypname().compareTo("bool") != 0)
                throw new AssertionError("AND/OR requires bool expression");
        }
    }

    @Override
    public String toString() {
        return "(" + left.toString() + ") " + op.getName() + " (" + right.toString() + ")";
    }

    PostgresGlobalState globalState;
    PgOperators.PgOperator2 op;
    PostgresExpression left;
    PostgresExpression right;
}
