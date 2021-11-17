package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;

public class PostgresTableRef extends PostgresExpression{
    @Override
    public PgType getExpressionType() {
        throw new AssertionError("get type on TableRef");
    }
    public static class NormalTable {
        String schema;
        String tablename;
    }
    public static class Select
}
