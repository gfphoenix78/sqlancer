package sqlancer.postgres.gen;

import java.util.*;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;

public class PostgresTableGenerator {

    private final String tableName;
    private boolean columnCanHavePrimaryKey;
    private boolean columnHasPrimaryKey;
    private boolean columnHasUnique;
//    private final StringBuilder sb = new StringBuilder();
    private boolean isTemporaryTable;
    private boolean ifNotExists;
    private TableType tableType;
    private final PostgresSchema newSchema;
    private final List<PostgresColumn> columns = new ArrayList<>();
    private List<String> inherits;
    private CreateLike createLike;
    private PartitionBy partitionBy;
    protected final ExpectedErrors errors = new ExpectedErrors();
    private final PostgresTable table;
    private final boolean generateOnlyKnown;
    private final PostgresGlobalState globalState;

    public PostgresTableGenerator(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        this.tableName = tableName;
        this.newSchema = newSchema;
        this.generateOnlyKnown = generateOnlyKnown;
        this.globalState = globalState;
        table = new PostgresTable(tableName, columns, null, null, null, false, false);
        errors.add("invalid input syntax for");
        errors.add("is not unique");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("cannot create partitioned table as inheritance child");
        errors.add("cannot cast");
        errors.add("ERROR: functions in index expression must be marked IMMUTABLE");
        errors.add("functions in partition key expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not exist for access method");
        errors.add("does not accept data type");
        errors.add("but default expression is of type text");
        errors.add("has pseudo-type unknown");
        errors.add("no collation was derived for partition key column");
        errors.add("inherits from generated column but specifies identity");
        errors.add("inherits from generated column but specifies default");
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonTableErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        return new PostgresTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate().generateQuery();
    }
//    private enum TableType {
//        STANDARD, TEMP, UNLOGGED
//    }
    private PostgresTableGenerator generate() {
        tableType = Randomly.fromList(TableType.values(), new int[]{85, 15, 5});
        tableType = Randomly.fromOptions(TableType.values());
        ifNotExists = Randomly.getBoolean(65, 30);
        if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
            createLike();
        } else {
            createStandard();
        }
        return this;
    }
    private SQLQueryAdapter generateQuery() {
        StringBuilder sb = new StringBuilder();
        columnCanHavePrimaryKey = true;
        isTemporaryTable = Randomly.getBoolean();
        tableType = Randomly.fromOptions(TableType.values());
        sb.append("CREATE ");
        if (tableType != TableType.STANDARD)
            sb.append(tableType.name()).append(' ');
        sb.append(" TABLE ");
        if (ifNotExists) {
            sb.append(" IF NOT EXISTS ");
        }
        sb.append(tableName);
        if (this.createLike != null) {
            assert columns.size() == 0;
            sb.append("(").append(this.createLike.generateQuery()).append(")");
        } else {
            // table columns
            sb.append("(");
            String ss = columns.stream().map(c -> {
                StringBuilder ssb = new StringBuilder();
                ssb.append(c.getName()).append(' ');
                PostgresCommon.appendDataType(c.getType(), ssb, false, generateOnlyKnown, globalState.getCollates());
                List<PostgresSchema.ColumnConstraint> constraints = c.getConstraints();
                if (constraints != null && constraints.size() > 0) {
                    ssb.append(' ');
                    String colConstraint = constraints.stream().
                            map(PostgresSchema.ColumnConstraint::generateQuery).
                            collect(Collectors.joining(" "));
                    ssb.append(colConstraint);
                }
                return ssb.toString();
            }).collect(Collectors.joining(", "));
            sb.append(ss);
            sb.append(")");
            // inherits
            if (this.inherits != null) {
                assert this.inherits.size() > 0;
                String inheritsItem = this.inherits.stream().collect(Collectors.joining(", "));
                sb.append("\nINHERITS (").append(inheritsItem).append(")");
            }
            // partition
            if (this.partitionBy != null) {
                sb.append("\n").append(this.partitionBy.generate());
            }
        }
        System.out.println(sb);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void createStandard() throws AssertionError {
//        sb.append("(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
//            if (i != 0) {
//                sb.append(", ");
//            }
            String name = DBMSCommon.createColumnName(i);
            createColumn(name);
        }
        if (Randomly.getBoolean()) {
            errors.add("constraints on temporary tables may reference only temporary tables");
            errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
            errors.add("constraints on permanent tables may reference only permanent tables");
            errors.add("cannot be implemented");
            errors.add("there is no unique constraint matching given keys for referenced table");
            errors.add("cannot reference partitioned table");
            errors.add("unsupported ON COMMIT and foreign key combination");
            errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
            errors.add("exclusion constraints are not supported on partitioned tables");
            PostgresCommon.addTableConstraints(columnHasPrimaryKey, columnHasUnique, table, globalState, errors);
        }
//        sb.append(")");
        generateInherits();
        if (PostgresProvider.majorVersion() >= 12)
            generatePartitionBy();
//        PostgresCommon.generateWith(sb, globalState, errors);
//        if (Randomly.getBoolean() && isTemporaryTable) {
//            sb.append(" ON COMMIT ");
//            sb.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS", "DROP"));
//            sb.append(" ");
//        }
    }

    private enum DistributionPolicy {
        BYKEYS, RANDOMLY, REPLICATED
    }
    private static class DistributedyBy {
        DistributionPolicy policy;
        List<Integer> colIndexes;
        List<PostgresColumn> columns;
        DistributedyBy(DistributionPolicy policy) {
            this.policy = policy;
        }
        void setDistributedKeys(List<Integer> list, List<PostgresColumn> cols) {
            this.colIndexes = list;
            this.columns = columns;
        }
        String generateQuery() {
            switch (policy) {
                case RANDOMLY:
                    return "DISTRIBUTED RANDOMLY";
                case REPLICATED:
                    return "DISTRIBUTED REPLICATED";
                case BYKEYS:
                    break;
                default:
                    throw new AssertionError("Unknown policy=" + policy);
            }
            // DISTRIBYTED BY (COLNAME [, ...])
            StringBuilder sb = new StringBuilder();
            sb.append("DISTRIBUTED BY (");
            String fields = this.colIndexes.stream().map(idx -> this.columns.get(idx).getName()).collect(Collectors.joining(", "));
            sb.append(fields);
            sb.append(")");
            return sb.toString();
        }
    }
    private enum CreateLikeOption {
        COMMENTS, CONSTRAINTS, DEFAULTS, GENERATED, IDENTITY,
        INDEXES, STATISTICS, STORAGE, ALL
    }
    private static class CreateLike {
        String likeTable;
        final List<CreateLikeOption> options = new ArrayList<>();
        final List<Boolean> includings = new ArrayList<>();
        CreateLike(String likeTable) {
            this.likeTable = likeTable;
        }
        void addOption(boolean include, CreateLikeOption option) {
            assert option != null;
            this.options.add(option);
            this.includings.add(include);
        }
        String generateQuery() {
            StringBuilder sb = new StringBuilder();
            sb.append("LIKE ").append(likeTable);
            for (int i = 0, n = options.size(); i < n; i++) {
                if (includings.get(i).booleanValue())
                    sb.append(" INCLUDING ");
                else
                    sb.append(" EXCLUDING ");
                sb.append(options.get(i).name());
            }
            return sb.toString();
        }
    }
    private static final CreateLikeOption []like_options_low = {
        CreateLikeOption.DEFAULTS,
        CreateLikeOption.CONSTRAINTS,
        CreateLikeOption.INDEXES,
        CreateLikeOption.STORAGE,
        CreateLikeOption.COMMENTS,
        CreateLikeOption.ALL,
    };
    private CreateLike createLike() {
        CreateLike createLike = new CreateLike(newSchema.getRandomTable().getName());
        if (Randomly.getBoolean()) {
            int majorVersion = PostgresProvider.majorVersion();
            List<CreateLikeOption> options = Arrays.asList(CreateLikeOption.values());
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                if (majorVersion < 12) {
                    createLike.addOption(Randomly.getBoolean(), Randomly.fromOptions(like_options_low));
                } else {
                    createLike.addOption(Randomly.getBoolean(), Randomly.fromList(options));
                }
            }
        }
        this.createLike = createLike;
        return createLike;
    }

    private void createColumn(String name) throws AssertionError {
//        sb.append(" ");
        PostgresDataType type = PostgresDataType.getRandomType();
//        boolean serial = PostgresCommon.appendDataType(type, sb, true, generateOnlyKnown, globalState.getCollates());
        PostgresColumn c = new PostgresColumn(name, type);
        c.setTable(table);
        columns.add(c);
//        sb.append(" ");
        if (Randomly.getBoolean()) {
            c.setConstraints(createColumnConstraint(type, false));
        }
    }
    private enum PartitionType {
        RANGE, LIST, HASH
    }
    private static class PartitionBy {
        PartitionType partitionType; // RANGE, LIST or HASH
        final List<Object> partitionList = new ArrayList<>();
        List<PostgresColumn> columns;
        PartitionBy(PostgresGlobalState globalState, PartitionType partitionType, List<PostgresColumn> columns, ExpectedErrors errors) {
            assert partitionType != null;
            assert columns != null;
            this.partitionType = partitionType;
            this.columns = columns;
            int n = partitionType == PartitionType.LIST ? 1 : Randomly.smallNumber() + 1;
            errors.add("unrecognized parameter");
            errors.add("cannot use constant expression");
            errors.add("cannot use constant expression as partition key");
            errors.add("cannot add NO INHERIT constraint to partitioned table");
            errors.add("unrecognized parameter");
            errors.add("unsupported PRIMARY KEY constraint with partition key definition");
            errors.add("which is part of the partition key.");
            errors.add("unsupported UNIQUE constraint with partition key definition");
            errors.add("does not accept data type");
            errors.add("does not exist for access method");
            PostgresCommon.addCommonExpressionErrors(errors);
            // partition by XXX (column_name | (expr),  ...)
            List<String> column_names = columns.stream().map(t -> t.getName()).collect(Collectors.toList());
            for (int i = 0; i < n; i++) {
                if (column_names.size() > 0 && Randomly.getBoolean()) {
                    // by name
                    String colname = Randomly.fromList(column_names);
                    partitionList.add(colname);
                    column_names.remove(colname);
                } else {
                    PostgresExpression expr = PostgresExpressionGenerator.generateExpression(globalState, columns);
                    partitionList.add(expr);
                }
            }
        }

        String generate() {
            StringBuilder sb = new StringBuilder();
            String s = partitionList.stream().map(t -> {
                if (t.getClass() == String.class) {
                    return (String)t;
                } else if (t instanceof PostgresExpression) {
                    return "(" + PostgresVisitor.asString((PostgresExpression) t) + ")";
                } else {
                    throw new AssertionError("invalid partition value type:"+t);
                }
            }).collect(Collectors.joining(", "));
            sb.append("PARTITION BY ")
                    .append(partitionType.name())
                    .append("(")
                    .append(s)
                    .append(")");
            return sb.toString();
        }
    }
    private void generatePartitionBy() {
        if (Randomly.getBoolean()) {
            return;
        }
        this.partitionBy = new PartitionBy(globalState, Randomly.fromOptions(PartitionType.values()), this.columns, this.errors);
//        sb.append(" PARTITION BY ");
//        // TODO "RANGE",
//        String partitionOption = Randomly.fromOptions("RANGE", "LIST", "HASH");
//        sb.append(partitionOption);
//        sb.append("(");
//        errors.add("unrecognized parameter");
//        errors.add("cannot use constant expression");
//        errors.add("cannot add NO INHERIT constraint to partitioned table");
//        errors.add("unrecognized parameter");
//        errors.add("unsupported PRIMARY KEY constraint with partition key definition");
//        errors.add("which is part of the partition key.");
//        errors.add("unsupported UNIQUE constraint with partition key definition");
//        errors.add("does not accept data type");
//        int n = partitionOption.contentEquals("LIST") ? 1 : Randomly.smallNumber() + 1;
//        PostgresCommon.addCommonExpressionErrors(errors);
//        for (int i = 0; i < n; i++) {
//            if (i != 0) {
//                sb.append(", ");
//            }
//            sb.append("(");
//            PostgresExpression expr = PostgresExpressionGenerator.generateExpression(globalState, columns);
//            sb.append(PostgresVisitor.asString(expr));
//            sb.append(")");
//            if (Randomly.getBoolean()) {
//                sb.append(globalState.getRandomOpclass());
//                errors.add("does not exist for access method");
//            }
//        }
//        sb.append(")");
    }

    private void generateInherits() {
        if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
//            sb.append(" INHERITS(");
            inherits = newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName()).collect(Collectors.toList());
//            sb.append(newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
//                    .collect(Collectors.joining(", ")));
//            sb.append(")");
            errors.add("has a type conflict");
            errors.add("has a generation conflict");
            errors.add("cannot create partitioned table as inheritance child");
            errors.add("cannot inherit from temporary relation");
            errors.add("cannot inherit from partitioned table");
            errors.add("has a collation conflict");
            errors.add("inherits conflicting default values");
            errors.add("specifies generation expression");
        }
    }



    private List<PostgresSchema.ColumnConstraint> createColumnConstraint(PostgresDataType type, boolean serial) {
        List<PostgresSchema.ColumnConstraintType> constraintSubset = Randomly.nonEmptySubset(PostgresSchema.ColumnConstraintType.values());
        List<PostgresSchema.ColumnConstraint> list = new ArrayList<>();
        if (PostgresProvider.majorVersion() < 12) {
            // PG < 12 doesn't support GENERATED syntax or different from PG 12.
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.GENERATED);
            while (constraintSubset.isEmpty()) {
                constraintSubset = Randomly.nonEmptySubset(PostgresSchema.ColumnConstraintType.values());
                constraintSubset.remove(PostgresSchema.ColumnConstraintType.GENERATED);
            }
        }
        if (Randomly.getBoolean()) {
            // make checks constraints less likely
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.CHECK);
        }
        if (!columnCanHavePrimaryKey || columnHasPrimaryKey) {
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.PRIMARY_KEY);
        }
        if (columnHasUnique) {
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.UNIQUE);
        }
        if (constraintSubset.contains(PostgresSchema.ColumnConstraintType.GENERATED)
                && constraintSubset.contains(PostgresSchema.ColumnConstraintType.DEFAULT)) {
            // otherwise: ERROR: both default and identity specified for column
            constraintSubset.remove(Randomly.fromOptions(PostgresSchema.ColumnConstraintType.GENERATED, PostgresSchema.ColumnConstraintType.DEFAULT));
        }
        if (constraintSubset.contains(PostgresSchema.ColumnConstraintType.GENERATED) && type != PostgresDataType.INT) {
            // otherwise: ERROR: identity column type must be smallint, integer, or bigint
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.GENERATED);
        }
        if (serial) {
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.GENERATED);
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.DEFAULT);
            constraintSubset.remove(PostgresSchema.ColumnConstraintType.NULL_OR_NOT_NULL);
        }
        for (PostgresSchema.ColumnConstraintType tp : constraintSubset) {
            PostgresExpression expression = null;
            switch (tp) {
                case UNIQUE:
                case PRIMARY_KEY:
                case NULL_OR_NOT_NULL:
                    errors.add("conflicting NULL/NOT NULL declarations");
                    break;
                case GENERATED:
                    if (Randomly.getBoolean())
                        break;
                    errors.add("A generated column cannot reference another generated column.");
                    errors.add("cannot use generated column in partition key");
                    errors.add("generation expression is not immutable");
                    errors.add("cannot use column reference in DEFAULT expression");
                    expression = PostgresExpressionGenerator.generateExpression(globalState, columns, type);
                    break;
                case CHECK:
                    errors.add("out of range");
                    expression = PostgresExpressionGenerator.generateExpression(globalState,
                            columns, PostgresDataType.BOOLEAN);
                    break;
                case DEFAULT:
                    errors.add("out of range");
                    errors.add("is a generated column");
                    expression = PostgresExpressionGenerator.generateExpression(globalState, type);
                    break;
            }
            list.add(new PostgresSchema.ColumnConstraint(tp, expression));
        }
//        for (ColumnConstraint c : constraintSubset) {
//            sb.append(" ");
//            switch (c) {
//            case NULL_OR_NOT_NULL:
//                sb.append(Randomly.fromOptions("NOT NULL", "NULL"));
//                errors.add("conflicting NULL/NOT NULL declarations");
//                break;
//            case UNIQUE:
//                sb.append("UNIQUE");
//                columnHasUnique = true;
//                break;
//            case PRIMARY_KEY:
//                sb.append("PRIMARY KEY");
//                columnHasPrimaryKey = true;
//                break;
//            case DEFAULT:
//                sb.append("DEFAULT");
//                sb.append(" (");
//                sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState, type)));
//                sb.append(")");
//                // CREATE TEMPORARY TABLE t1(c0 smallint DEFAULT ('566963878'));
//                errors.add("out of range");
//                errors.add("is a generated column");
//                break;
//            case CHECK:
//                sb.append("CHECK (");
//                sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState,
//                        columnsToBeAdded, PostgresDataType.BOOLEAN)));
//                sb.append(")");
//                if (Randomly.getBoolean()) {
//                    sb.append(" NO INHERIT");
//                }
//                errors.add("out of range");
//                break;
//            case GENERATED:
//                sb.append("GENERATED ");
//                if (Randomly.getBoolean()) {
//                    sb.append(" ALWAYS AS (");
//                    sb.append(PostgresVisitor.asString(
//                            PostgresExpressionGenerator.generateExpression(globalState, columnsToBeAdded, type)));
//                    sb.append(") STORED");
//                    errors.add("A generated column cannot reference another generated column.");
//                    errors.add("cannot use generated column in partition key");
//                    errors.add("generation expression is not immutable");
//                    errors.add("cannot use column reference in DEFAULT expression");
//                } else {
//                    sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
//                    sb.append(" AS IDENTITY");
//                }
//                break;
//            default:
//                throw new AssertionError(sb);
//            }
//        }
        return list;
    }
}
