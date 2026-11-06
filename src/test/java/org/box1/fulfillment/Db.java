package org.box1.fulfillment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Direct reads of the adapter's database for assertions, on a connection outside the application's pool. */
public final class Db {
    private Db() {
    }

    public static List<List<Object>> query(String sql, Object... args) {
        try (Connection c = DriverManager.getConnection(FullStackResource.postgres.getJdbcUrl(),
                FullStackResource.postgres.getUsername(), FullStackResource.postgres.getPassword());
                PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            List<List<Object>> rows = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                int n = r.getMetaData().getColumnCount();
                while (r.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= n; i++) {
                        row.add(r.getObject(i));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Object one(String sql, Object... args) {
        List<List<Object>> rows = query(sql, args);
        return rows.isEmpty() ? null : rows.get(0).get(0);
    }

    public static void exec(String sql, Object... args) {
        try (Connection c = DriverManager.getConnection(FullStackResource.postgres.getJdbcUrl(),
                FullStackResource.postgres.getUsername(), FullStackResource.postgres.getPassword());
                PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            s.execute();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
