package sqlancer.postgres.ast2;

import com.mysql.cj.exceptions.AssertionFailedException;
import sqlancer.postgres.PgType;

public class PostgresCaseExpr implements PostgresExpression {


    // cond_values are: cond1, value1, cond2, value2, ...
    public PostgresCaseExpr(PostgresExpression defaultValue, PostgresExpression ...cond_values) {
        if (cond_values.length < 2 || cond_values.length % 2 != 0)
            throw new AssertionError("CASE condition and value must match");
        this.defaultValue = defaultValue;
        this.conditions = new PostgresExpression[cond_values.length/2];
        this.values = new PostgresExpression[cond_values.length/2];
        for (int i = 0, n = cond_values.length; i < n; i += 2) {
            this.conditions[i/2] = cond_values[i];
            this.values[i/2] = cond_values[i+1];
        }
        validate();
    }
    void validate() {
        PgType type = getExpressionType();
        for (int i = 0, n = conditions.length; i < n; i++) {
            if (conditions[i] == null)
                throw new AssertionError(String.format("CASE: condition[%d] is null"));
            if (conditions[i].getExpressionType().getTypname().compareTo("bool") != 0)
                throw new AssertionError("CASE: condition expr is not bool:" + conditions[i]);
            if (values[i] != null &&
                    values[i].getExpressionType().getTypname().compareTo(type.getTypname()) != 0)
                throw new AssertionError("CASE: value types not match");
        }
    }

    @Override
    public PgType getExpressionType() {
        if (defaultValue != null)
            return defaultValue.getExpressionType();
        for (PostgresExpression v : values) {
            if (v != null)
                return v.getExpressionType();
        }
        throw new AssertionError("CASE: all values are null");
    }

    PostgresExpression []conditions;
    PostgresExpression []values;
    PostgresExpression defaultValue;
}
