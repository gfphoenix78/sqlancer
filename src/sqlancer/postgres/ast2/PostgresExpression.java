package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;

public interface PostgresExpression {
    PgType getExpressionType();
}
