package org.box1.fulfillment.camel;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileFilter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Accepts consumed/ files older than the retention period; the {@code consumed-retention} route's {@code filter}. */
@Named("consumedRetentionFilter")
@ApplicationScoped
public class ConsumedRetentionFilter implements GenericFileFilter<Object> {
    private final Duration retention;

    public ConsumedRetentionFilter(@ConfigProperty(name = "fulfillment.consumed.retention") Duration retention) {
        this.retention = retention;
    }

    @Override
    public boolean accept(GenericFile<Object> file) {
        return !file.isDirectory() && file.getLastModified() > 0
                && file.getLastModified() < System.currentTimeMillis() - retention.toMillis();
    }
}
