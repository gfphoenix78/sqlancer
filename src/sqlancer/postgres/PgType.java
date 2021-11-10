package sqlancer.postgres;

import java.util.HashMap;
import java.util.Map;

public class PgType {
    String typname;
    int oid;
    int typrelid;
    int typelem;
    int typarray;
    char typdelim;
    char typtype;

    public PgType(String typname,
            int oid,
            int typrelid,
            int typelem,
            int typarray,
            char typdelim,
            char typtype) {
        this.typname = typname;
        this.oid = oid;
        this.typrelid = typrelid;
        this.typelem = typelem;
        this.typarray = typarray;
        this.typdelim = typdelim;
        this.typtype = typtype;
    }
//    public static PgType addType(String typname,
//                          int oid,
//                          int typrelid,
//                          int typelem,
//                          int typarray,
//                          char typdelim,
//                          char typtype) {
//        PgType t = getType(typname);
//        if (t != null)
//            throw new RuntimeException("type "+typname+" already exists");
//        t = new PgType(typname, oid, typrelid, typelem, typarray, typdelim, typtype);
//        name2types.put(typname, t);
//        return t;
//    }
//    public static PgType addAlias(String alias, String ref) {
//        PgType t;
//        t = getType(alias);
//        if (t != null) {
//            throw new RuntimeException("type already exists:"+alias);
//        }
//        t = getType(ref);
//        if (t == null)
//            throw new RuntimeException("Can't find type for alias:"+ref);
//        name2types.put(alias, t);
//        return t;
//    }
//    public static PgType getType(String name) {
//        return name2types.get(name);
//    }
//    static final Map<String, PgType> name2types = new HashMap<>();
}
