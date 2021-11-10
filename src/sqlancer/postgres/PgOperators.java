package sqlancer.postgres;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PgOperators {

    public static abstract class PgOperator {
        String name;
        PgType result;
        PgOperator(String name, PgType result) {
            this.name = name;
            this.result = result;
        }
    }
    public static class PgOperator1 extends PgOperator {
        PgOperator1(String name, PgType result, PgType operand) {
            super(name, result);
            this.operand = operand;
        }
        PgType operand;
    }
    public static class PgOperator2 extends PgOperator {
        PgOperator2(String name, PgType result, PgType left, PgType right) {
            super(name, result);
            this.left = left;
            this.right = right;
        }
        PgType left, right;
    }
    public List<PgOperator1> getUnaryOperators() {
        return unaryOperators;
    }
    public List<PgOperator2> getBinaryOperators() {
        return binaryOperators;
    }
    public void addOperator(String name, PgType result, PgType left, PgType right) {
        assert result != null && right != null;
        assert name != null && name.length() > 0;
        if (left == null)
            unaryOperators.add(new PgOperator1(name, result, right));
        else
            binaryOperators.add(new PgOperator2(name, result, left, right));
    }
    final List<PgOperator1> unaryOperators = new ArrayList<>();
    final List<PgOperator2> binaryOperators = new ArrayList<>();
}
