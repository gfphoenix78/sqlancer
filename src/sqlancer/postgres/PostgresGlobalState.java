package sqlancer.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;

public class PostgresGlobalState extends SQLGlobalState<PostgresOptions, PostgresSchema> {

    public static final char IMMUTABLE = 'i';
    public static final char STABLE = 's';
    public static final char VOLATILE = 'v';

//    private List<String> operators = Collections.emptyList();
    private List<String> collates = Collections.emptyList();
    private List<String> opClasses = Collections.emptyList();
    private List<String> tableAccessMethods = Collections.emptyList();
    // store and allow filtering by function volatility classifications
    private final Map<String, Character> functionsAndTypes = new HashMap<>();
    private final Map<String, PgType> name2types = new HashMap<>();
    private final Map<Integer, PgType> oid2types = new HashMap<>();
    private final PgOperators pgOperators = new PgOperators();
    private final PgProcs pgProcs = new PgProcs();
    private List<Character> allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);

    @Override
    public void setConnection(SQLConnection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses(getConnection());
//            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
            this.tableAccessMethods = getTableAccessMethods(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private List<String> getCollnames(SQLConnection con) throws SQLException {
        List<String> collNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
                while (rs.next()) {
                    collNames.add(rs.getString(1));
                }
            }
        }
        return collNames;
    }

    private List<String> getOpclasses(SQLConnection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

//    private List<String> getOperators(SQLConnection con) throws SQLException {
//        List<String> operators = new ArrayList<>();
//        try (Statement s = con.createStatement()) {
//            try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
//                while (rs.next()) {
//                    operators.add(rs.getString(1));
//                }
//            }
//        }
//        return operators;
//    }

    public PgOperators getPgOperators() {
        return pgOperators;
    }
    public PgProcs getPgProcs() {
        return pgProcs;
    }
    private List<String> getTableAccessMethods(SQLConnection con) throws SQLException {
        List<String> tableAccessMethods = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            /*
             * pg_am includes both index and table access methods so we need to filter with amtype = 't'
             */
            try (ResultSet rs = s.executeQuery("SELECT amname FROM pg_am WHERE amtype = 't';")) {
                while (rs.next()) {
                    tableAccessMethods.add(rs.getString(1));
                }
            }
        }
        return tableAccessMethods;
    }

    public List<String> getOperators() {
        return pgOperators.binaryOperators.stream().map(op -> op.name).collect(Collectors.toList());
    }
//
//    public String getRandomOperator() {
//        return Randomly.fromList(operators);
//    }

    public Map<String, PgType> getTypes() { return name2types; }
    public List<String> getCollates() {
        return collates;
    }

    public String getRandomCollate() {
        return Randomly.fromList(collates);
    }

    public List<String> getOpClasses() {
        return opClasses;
    }

    public String getRandomOpclass() {
        return Randomly.fromList(opClasses);
    }

    public List<String> getTableAccessMethods() {
        return tableAccessMethods;
    }

    public String getRandomTableAccessMethod() {
        return Randomly.fromList(tableAccessMethods);
    }

    @Override
    public PostgresSchema readSchema() throws SQLException {
        return PostgresSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public void addFunctionAndType(String functionName, Character functionType) {
        this.functionsAndTypes.put(functionName, functionType);
    }

    public void addType(PgType pgType) {
        if (name2types.get(pgType.typname) != null)
            throw new RuntimeException("type "+pgType.typname+" already exists");
        if (oid2types.get(pgType.oid) != null)
            throw new RuntimeException("type with oid="+pgType.oid+" already exists");
        name2types.put(pgType.typname, pgType);
        oid2types.put(pgType.oid, pgType);
    }
    public void addTypeAlias(String alias, String ref) {
        PgType t = name2types.get(alias);
        if (t != null)
            throw new RuntimeException("type/alias "+alias+" already exists");
        t = name2types.get(ref);
        if (t == null)
            throw new RuntimeException("type "+ref+" doesn't exist");
        name2types.put(alias, t);
    }

    public PgType getBoolType() {
        return getType("bool");
    }
    public PgType getType(String name) { return name2types.get(name); }
    public PgType getType(int oid) { return oid2types.get(oid); }
    public Map<String, Character> getFunctionsAndTypes() {
        return this.functionsAndTypes;
    }

    public void setAllowedFunctionTypes(List<Character> types) {
        this.allowedFunctionTypes = types;
    }

    public void setDefaultAllowedFunctionTypes() {
        this.allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);
    }

    public List<Character> getAllowedFunctionTypes() {
        return this.allowedFunctionTypes;
    }

}
