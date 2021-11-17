package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;

public class PostgresPostfixExpr implements PostgresExpression {
    public enum PostfixOperator {
        IS_NULL("IS NULL", "ISNULL") {

        },
        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {

        },
        IS_UNKNOWN("IS UNKNOWN") {

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {

        },
        IS_TRUE("IS TRUE") {

        },
        IS_FALSE("IS FALSE") {

        },
        ;
        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations;
        }
        private String[] textRepresentations;
    }

    @Override
    public PgType getExpressionType() {
        return globalState.getType("bool");
    }

    @Override
    public String toString() {
        return expr.toString() + " " + operator.textRepresentations[0];
    }

    public PostgresPostfixExpr(PostgresGlobalState globalState, PostfixOperator operator, PostgresExpression expr) {
        this.globalState = globalState;
        this.operator = operator;
        this.expr = expr;
    }
    PostgresGlobalState globalState;
    PostfixOperator operator;
    PostgresExpression expr;
}
