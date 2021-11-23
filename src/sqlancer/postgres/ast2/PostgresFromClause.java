package sqlancer.postgres.ast2;

import java.util.ArrayList;
import java.util.List;

public class PostgresFromClause {
    // L[0].tableRef L[0].joinType L[1].tableRef L[1].joinType L[2].tableRef ...
    // L[len-1].joinType is ignored.
    final List<PostgresJoinElement> joinElementList = new ArrayList<>();
}
