package net.omisoft.cassandra.migrations;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import io.smartcat.migration.MigrationEngine;
import io.smartcat.migration.MigrationResources;
import io.smartcat.migration.SchemaMigration;
import io.smartcat.migration.exceptions.MigrationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;

@Component
@RequiredArgsConstructor
@Slf4j
public class MigrationManager {

    private final Session session;

    @Value("${spring.data.cassandra.keyspace-name}")
    private String keyspace;

    @PostConstruct
    public void setup() {
        doMigration();
    }

    private void doMigration() {
        Assert.notNull(session, "Session object is null");
        Assert.hasText(keyspace, "Keyspace cannot be null or empty");
        printMetadata(session);
        migrateSchema(session);
    }

    private void printMetadata(final Session session) {
        final Metadata metadata = session.getCluster().getMetadata();
        log.info("Connected to cluster = {}", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts()) {
            log.info("Datacenter = {} host = {}", host.getDatacenter(), host.getAddress());
        }
    }

    private void migrateSchema(final Session session) {
        log.info("Executing schema migrations");
        final MigrationResources resources = getMigrationResources();
        MigrationEngine.withSession(session).migrate(resources);
        log.info("Done with schema migrations");
    }

    private MigrationResources getMigrationResources() {
        MigrationResources resources = new MigrationResources();
        resources.addMigration(new SchemaMigration(1) {

            @Override
            public String getDescription() {
                return "Create table products";
            }

            @Override
            public void execute() throws MigrationException {
                String query = SchemaBuilder
                        .createTable("products")
                        .addPartitionKey("id", DataType.uuid())
                        .addColumn("name", DataType.text())
                        .addColumn("price", DataType.bigint())
                        .ifNotExists()
                        .withOptions()
                        .comment("Products table created")
                        .buildInternal();
                log.info("!!!!!!!!!!!! {}", query);
                executeWithSchemaAgreement(new SimpleStatement(query));
            }

        });
        return resources;
    }

}