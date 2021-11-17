package sqlancer.postgres.ast2;

import sqlancer.postgres.PgType;

import java.util.stream.Collectors;
import java.util.stream.Stream;

// implement PostgresGroupElement as PostgresExpression, because
// grouping sets may accept grouping sets as well as expressions.
public class PostgresGroupElement implements PostgresExpression {
    public enum GroupType {
        NONE, // ()
        EXPR, // expr or expr, expr, ...
        ROLLUP, // ROLLUP(expr [, ...])
        CUBE, // CUBE(expr [, ...])
        GROUPING_SETS; // GROUPING SETS(group_element [, ...])
    }
    public PostgresGroupElement(GroupType groupType, PostgresExpression ...expressions) {
        this.groupType = groupType;
        this.expressions = expressions;
        if (groupType != GroupType.GROUPING_SETS) {
            for (PostgresExpression x : expressions)
                if (x instanceof PostgresGroupElement)
                    throw new AssertionError("GroupElement is allowed in grouping sets");
        }
    }

    @Override
    public PgType getExpressionType() {
        throw new UnsupportedOperationException("No value for group element");
    }

    private String joinExprs() {
        return Stream.of(expressions).map(e -> e.toString()).collect(Collectors.joining(", "));
    }
    // append to GROUP BY <...>
    @Override
    public String toString() {
        switch (groupType) {
            case NONE:
                return "()";
            case EXPR:
                return "(" + joinExprs() + ")";
            case ROLLUP:
                return "ROLLUP(" + joinExprs() + ")";
            case CUBE:
                return "CUBE(" + joinExprs() + ")";
            case GROUPING_SETS:
                return "GROUPING SETS("+joinExprs() + ")";
            default:
                throw new AssertionError("Invalid group type:"+groupType);
        }
    }

    GroupType groupType;
    PostgresExpression []expressions;
}
