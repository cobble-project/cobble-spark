package io.cobble.spark;

import io.cobble.table.CatalogTable;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableSchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Driver-authenticated catalog identity carried to Spark tasks.
 *
 * <p>This is deliberately not parsed from datasource options: the native catalog produced the
 * immutable table and schema ids, so a stale task cannot silently write a renamed or recreated
 * table.
 */
public final class CobbleCatalogReference implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String warehouse;
    private final String storageId;
    private final List<String> namespace;
    private final String name;
    private final long tableId;
    private final long catalogSchemaId;
    private final TableSchema schema;

    public CobbleCatalogReference(
            String warehouse,
            String storageId,
            List<String> namespace,
            String name,
            long tableId,
            long catalogSchemaId,
            TableSchema schema) {
        this.warehouse = Objects.requireNonNull(warehouse, "warehouse");
        this.storageId = Objects.requireNonNull(storageId, "storageId");
        this.namespace =
                Collections.unmodifiableList(
                        new ArrayList<String>(Objects.requireNonNull(namespace, "namespace")));
        this.name = Objects.requireNonNull(name, "name");
        this.tableId = tableId;
        this.catalogSchemaId = catalogSchemaId;
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    public String warehouse() {
        return warehouse;
    }

    public String storageId() {
        return storageId;
    }

    public TableIdentifier identifier() {
        return new TableIdentifier(namespace, name);
    }

    /** Stable Spark display name, independent of the catalog warehouse location. */
    public String qualifiedName() {
        return String.join(".", namespace) + "." + name;
    }

    public long tableId() {
        return tableId;
    }

    public long catalogSchemaId() {
        return catalogSchemaId;
    }

    public TableSchema schema() {
        return schema;
    }

    /** Reject a stale reference after rename, drop/recreate, or catalog schema replacement. */
    public void validate(CatalogTable table) {
        if (table.tableId() != tableId || table.catalogSchemaId() != catalogSchemaId) {
            throw new IllegalStateException(
                    "catalog table identity changed since this Spark operation was planned");
        }
    }
}
