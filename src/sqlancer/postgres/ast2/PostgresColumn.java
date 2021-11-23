package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;

public class PostgresColumn {
    public PostgresColumn(String name, PgType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public PgType getType() {
        return type;
    }

    String name;
    PgType type;
}
