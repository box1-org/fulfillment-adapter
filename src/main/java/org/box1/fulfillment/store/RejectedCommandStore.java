package org.box1.fulfillment.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

/** Durable record of Kafka records that could not be accepted, written before they are dead-lettered. */
@ApplicationScoped
public class RejectedCommandStore {
    private final DataSource dataSource;

    public RejectedCommandStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void record(String eventId, String fulfillmentRequestId, String reason, String topic, int partition, long offset) {
        String sql = "INSERT INTO rejected_command (event_id, fulfillment_request_id, reason, topic, kafka_partition, kafka_offset)"
                + " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, eventId);
            s.setString(2, fulfillmentRequestId);
            s.setString(3, reason);
            s.setString(4, topic);
            s.setInt(5, partition);
            s.setLong(6, offset);
            s.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }
}
