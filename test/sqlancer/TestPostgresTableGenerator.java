package sqlancer;

import org.junit.jupiter.api.Test;
import sqlancer.postgres.PostgresSchema;

import java.util.Arrays;
import java.util.stream.Collectors;

public class TestPostgresTableGenerator {
    @Test
    public void testEnum() {
        PostgresSchema.PostgresTable.TableType []values = PostgresSchema.PostgresTable.TableType.values();
        String ss = Arrays.asList(values).stream().map(e -> e.name()).collect(Collectors.joining(", "));
        System.out.println(ss);
    }
}
