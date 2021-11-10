package sqlancer.postgres;

import java.util.ArrayList;
import java.util.List;

public class PgProcs {
    public static class PgProc {
        String name;
        String schema;
        int oid;
        List<PgType> args = new ArrayList<>();
        PgType rettype;
        public PgProc(String schema, String name, int oid, PgType rettype) {
            this.schema = schema;
            this.name = name;
            this.oid = oid;
            this.rettype = rettype;
        }
        public PgProc addArgTypes(PgType ...args) {
            for (int i = 0, n = args.length; i < n; i++)
                this.args.add(args[i]);
            return this;
        }
    }
    public PgProc addProc(String schema, String name, int oid, PgType rettype) {
        PgProc proc = new PgProc(schema, name, oid, rettype);
        this.pgProcs.add(proc);
        return proc;
    }

    final List<PgProc> pgProcs = new ArrayList<>();
}
