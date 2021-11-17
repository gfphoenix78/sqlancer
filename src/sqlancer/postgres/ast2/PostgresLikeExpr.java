package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;

public class PostgresLikeExpr implements PostgresExpression {
    @Override
    public PgType getExpressionType() {
        return globalState.getBoolType();
    }
    public PostgresLikeExpr(PostgresGlobalState globalState, PostgresExpression expr, boolean not, String pattern) {
        this.globalState = globalState;
        this.expr = expr;
        this.not = not;
        this.pattern = pattern;
        validate();
    }

    @Override
    public String toString() {
        String s = expr.toString();
        if (not)
            s += " NOT";
        s += " LIKE " + pattern;
        return s;
    }

    void validate() {
        if (this.expr.getExpressionType().getTypname().compareTo("text") != 0)
            throw new AssertionError("expr [NOT] LIKE <PATTERN> not valid type:" +
                    expr.getExpressionType());
    }

    PostgresGlobalState globalState;
    PostgresExpression expr;
    String pattern;
    boolean not;
}
