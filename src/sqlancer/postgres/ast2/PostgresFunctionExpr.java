package sqlancer.postgres.ast2;

import sqlancer.postgres.PgProcs;
import sqlancer.postgres.PgType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresFunctionExpr implements PostgresExpression {
    public PostgresFunctionExpr(PgProcs.PgProc proc, List<PostgresExpression> arguments) {
        this.proc = proc;
        if (proc.getArgs().size() != arguments.size()) {
            throw new IllegalArgumentException("number of arguments not match");
        }
        List<PgType> argTypes = proc.getArgs();
        this.arguments = new PostgresExpression[arguments.size()];
        for (int i = 0, n = arguments.size(); i < n; i++) {
            if (!argTypes.get(i).assignable(arguments.get(i).getExpressionType()))
                throw new IllegalArgumentException("unable to pass incompatible type for function");
            this.arguments[i] = arguments.get(i);
        }
    }
    @Override
    public PgType getExpressionType() {
        return proc.getRettype();
    }

    @Override
    public String toString() {
        return proc.getName() + "(" +
                Stream.of(arguments).map(Object::toString).collect(Collectors.joining(", ")) +
                ")";
    }

    final PostgresExpression []arguments;
    PgProcs.PgProc proc;
}
