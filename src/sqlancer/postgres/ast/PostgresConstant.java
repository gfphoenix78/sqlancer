package sqlancer.postgres.ast;

import java.math.BigDecimal;

import sqlancer.IgnoreMeException;
import sqlancer.postgres.PgType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public abstract class PostgresConstant implements PostgresExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    protected PostgresGlobalState globalState;
    protected PostgresConstant(PostgresGlobalState globalState) {
        this.globalState = globalState;
    }

    public static class BooleanConstant extends PostgresConstant {

        private final boolean value;

        public BooleanConstant(PostgresGlobalState globalState, boolean value) {
            super(globalState);
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public PgType getExpressionType() {
            return PostgresDataType.BOOLEAN;
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public PostgresConstant isEquals(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isBoolean()) {
                return PostgresConstant.createBooleanConstant(globalState, value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return PostgresConstant
                        .createBooleanConstant(globalState, value == rightVal.cast(PostgresDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected PostgresConstant isLessThan(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(PostgresDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return PostgresConstant.createBooleanConstant(globalState, (value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public PostgresConstant cast(PgType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return PostgresConstant.createIntConstant(globalState, value ? 1 : 0);
            case TEXT:
                return PostgresConstant.createTextConstant(globalState, value ? "true" : "false");
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class PostgresNullConstant extends PostgresConstant {

        public PostgresNullConstant(PostgresGlobalState globalState) {
            super(globalState);
        }
        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public PgType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public PostgresConstant isEquals(PostgresConstant rightVal) {
            return PostgresConstant.createNullConstant(globalState);
        }

        @Override
        protected PostgresConstant isLessThan(PostgresConstant rightVal) {
            return PostgresConstant.createNullConstant(globalState);
        }

        @Override
        public PostgresConstant cast(PostgresDataType type) {
            return PostgresConstant.createNullConstant(globalState);
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class StringConstant extends PostgresConstant {

        private final String value;

        public StringConstant(PostgresGlobalState globalState, String value) {
            super(globalState);
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public PostgresConstant isEquals(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isInt()) {
                return cast(PostgresDataType.INT).isEquals(rightVal.cast(PostgresDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(PostgresDataType.BOOLEAN).isEquals(rightVal.cast(PostgresDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return PostgresConstant.createBooleanConstant(globalState, value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected PostgresConstant isLessThan(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isInt()) {
                return cast(PostgresDataType.INT).isLessThan(rightVal.cast(PostgresDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(PostgresDataType.BOOLEAN).isLessThan(rightVal.cast(PostgresDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return PostgresConstant.createBooleanConstant(globalState, value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PostgresConstant cast(PgType type) {
            if (type == PostgresDataType.TEXT) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return PostgresConstant.createBooleanConstant(globalState, Long.parseLong(s) != 0);
                } catch (NumberFormatException e) {
                }
                switch (s.toUpperCase()) {
                case "T":
                case "TR":
                case "TRU":
                case "TRUE":
                case "1":
                case "YES":
                case "YE":
                case "Y":
                case "ON":
                    return PostgresConstant.createTrue(globalState);
                case "F":
                case "FA":
                case "FAL":
                case "FALS":
                case "FALSE":
                case "N":
                case "NO":
                case "OF":
                case "OFF":
                default:
                    return PostgresConstant.createFalse(globalState);
                }
            case INT:
                try {
                    return PostgresConstant.createIntConstant(globalState, Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return PostgresConstant.createIntConstant(globalState, -1);
                }
            case TEXT:
                return this;
            default:
                return null;
            }
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("text");
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

    }

    public static class IntConstant extends PostgresConstant {

        private final long val;

        public IntConstant(PostgresGlobalState globalState, long val) {
            super(globalState);
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("int4");
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public PostgresConstant isEquals(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isBoolean()) {
                return cast(PostgresDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return PostgresConstant.createBooleanConstant(globalState, val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return PostgresConstant.createBooleanConstant(globalState, val == rightVal.cast(PostgresDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected PostgresConstant isLessThan(PostgresConstant rightVal) {
            if (rightVal.isNull()) {
                return PostgresConstant.createNullConstant(globalState);
            } else if (rightVal.isInt()) {
                return PostgresConstant.createBooleanConstant(globalState, val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return PostgresConstant.createBooleanConstant(globalState, val < rightVal.cast(PostgresDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public PostgresConstant cast(PostgresDataType type) {
            switch (type) {
            case BOOLEAN:
                return PostgresConstant.createBooleanConstant(globalState, val != 0);
            case INT:
                return this;
            case TEXT:
                return PostgresConstant.createTextConstant(globalState, String.valueOf(val));
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static PostgresConstant createNullConstant(PostgresGlobalState globalState) {
        return new PostgresNullConstant(globalState);
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    public static PostgresConstant createIntConstant(PostgresGlobalState globalState, long val) {
        return new IntConstant(globalState, val);
    }

    public static PostgresConstant createBooleanConstant(PostgresGlobalState globalState, boolean val) {
        return new BooleanConstant(globalState, val);
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return this;
    }

    public boolean isNull() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public static PostgresConstant createFalse(PostgresGlobalState globalState) {
        return createBooleanConstant(globalState, false);
    }

    public static PostgresConstant createTrue(PostgresGlobalState globalState) {
        return createBooleanConstant(globalState, true);
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract PostgresConstant isEquals(PostgresConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract PostgresConstant isLessThan(PostgresConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract PostgresConstant cast(PostgresDataType type);

    public static PostgresConstant createTextConstant(PostgresGlobalState globalState, String string) {
        return new StringConstant(globalState, string);
    }

    public abstract static class PostgresConstantBase extends PostgresConstant {

        public PostgresConstantBase(PostgresGlobalState globalState) {
            super(globalState);
        }
        @Override
        public String getUnquotedTextRepresentation() {
            return null;
        }

        @Override
        public PostgresConstant isEquals(PostgresConstant rightVal) {
            return null;
        }

        @Override
        protected PostgresConstant isLessThan(PostgresConstant rightVal) {
            return null;
        }

        @Override
        public PostgresConstant cast(PgType type) {
            return null;
        }
    }

    public static class DecimalConstant extends PostgresConstantBase {

        private final BigDecimal val;

        public DecimalConstant(PostgresGlobalState globalState, BigDecimal val) {
            super(globalState);
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("numeric");
        }

    }

    public static class InetConstant extends PostgresConstantBase {

        private final String val;

        public InetConstant(PostgresGlobalState globalState, String val) {
            super(globalState);
            this.val = "'" + val + "'";
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("inet");
        }

    }

    public static class FloatConstant extends PostgresConstantBase {

        private final float val;

        public FloatConstant(PostgresGlobalState globalState, float val) {
            super(globalState);
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("float4");
        }

    }

    public static class DoubleConstant extends PostgresConstantBase {

        private final double val;

        public DoubleConstant(PostgresGlobalState globalState, double val) {
            super(globalState);
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("float8");
        }

    }

    public static class BitConstant extends PostgresConstantBase {

        private final long val;

        public BitConstant(PostgresGlobalState globalState, long val) {
            super(globalState);
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("B'%s'", Long.toBinaryString(val));
        }

        @Override
        public PgType getExpressionType() {
            return globalState.getType("bit");
        }

    }

    public static class RangeConstant extends PostgresConstantBase {

        private final long left;
        private final boolean leftIsInclusive;
        private final long right;
        private final boolean rightIsInclusive;

        public RangeConstant(PostgresGlobalState globalState, long left, boolean leftIsInclusive, long right, boolean rightIsInclusive) {
            super(globalState);
            this.left = left;
            this.leftIsInclusive = leftIsInclusive;
            this.right = right;
            this.rightIsInclusive = rightIsInclusive;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            if (leftIsInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            sb.append(left);
            sb.append(",");
            sb.append(right);
            if (rightIsInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            sb.append("'");
            sb.append("::int4range");
            return sb.toString();
        }

        @Override
        public PgType getExpressionType() {
            return PostgresDataType.RANGE;
        }

    }

    public static PostgresConstant createDecimalConstant(PostgresGlobalState globalState, BigDecimal bigDecimal) {
        return new DecimalConstant(globalState, bigDecimal);
    }

    public static PostgresConstant createFloatConstant(PostgresGlobalState globalState, float val) {
        return new FloatConstant(globalState, val);
    }

    public static PostgresConstant createDoubleConstant(PostgresGlobalState globalState, double val) {
        return new DoubleConstant(globalState, val);
    }

    public static PostgresConstant createRange(PostgresGlobalState globalState, long left, boolean leftIsInclusive, long right,
            boolean rightIsInclusive) {
        long realLeft;
        long realRight;
        if (left > right) {
            realRight = left;
            realLeft = right;
        } else {
            realLeft = left;
            realRight = right;
        }
        return new RangeConstant(globalState, realLeft, leftIsInclusive, realRight, rightIsInclusive);
    }

    public static PostgresExpression createBitConstant(long integer) {
        return new BitConstant(integer);
    }

    public static PostgresExpression createInetConstant(String val) {
        return new InetConstant(val);
    }

}
