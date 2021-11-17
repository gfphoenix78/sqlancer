package sqlancer.postgres;

import java.util.ArrayList;
import java.util.List;

public class PgProcs {
    public static class PgProc {
        String name;
        String schema;
        int oid;
        char prokind; // 'a' aggregate, 'f': normal function, 'p': procedure, 'w': window function
        char provolatile; // 'v': volatile, 's': stable, 'i': immutable
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

        public PgProc setProKind(char prokind) {
            this.prokind = prokind;
            return this;
        }

        public PgProc setProVolatile(char provolatile) {
            this.provolatile = provolatile;
            return this;
        }

        public char getProKind() {
            return prokind;
        }

        public char getProVolatile() {
            return provolatile;
        }

        public String getName() {
            return name;
        }

        public int getOid() {
            return oid;
        }

        public List<PgType> getArgs() {
            return args;
        }

        public PgType getRettype() {
            return rettype;
        }
    }
    public PgProc addProc(String schema, String name, int oid, PgType rettype) {
        PgProc proc = new PgProc(schema, name, oid, rettype);
        this.pgProcs.add(proc);
        return proc;
    }

    final List<PgProc> pgProcs = new ArrayList<>();
}
