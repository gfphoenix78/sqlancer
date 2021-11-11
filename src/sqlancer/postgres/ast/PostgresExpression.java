package sqlancer.postgres.ast;

import sqlancer.postgres.PgType;

public interface PostgresExpression {

    default PgType getExpressionType() {
        return null;
    }

    default PostgresConstant getExpectedValue() {
        return null;
    }
}
