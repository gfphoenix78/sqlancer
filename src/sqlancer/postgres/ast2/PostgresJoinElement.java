package sqlancer.postgres.ast2;

public class PostgresJoinElement {
    public enum PostgresJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;
    }
    PostgresExpression tableRef;
    PostgresJoinType joinType;
}
