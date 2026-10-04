package io.cobble.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.SparkSessionExtensionsProvider;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** Binds catalog snapshot options while DataFrameReader.table resolves its standalone relation. */
public final class CobbleSparkSessionExtensions
        extends AbstractFunction1<SparkSessionExtensions, BoxedUnit>
        implements SparkSessionExtensionsProvider {

    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectResolutionRule(
                new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
                    @Override
                    public Rule<LogicalPlan> apply(SparkSession session) {
                        return new BindCatalogSnapshot();
                    }
                });
        return BoxedUnit.UNIT;
    }

    static final class BindCatalogSnapshot extends Rule<LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            if (plan instanceof SubqueryAlias) {
                SubqueryAlias alias = (SubqueryAlias) plan;
                LogicalPlan child = apply(alias.child());
                return child == alias.child() ? alias : alias.copy(alias.identifier(), child);
            }
            // Do not rewrite embedded relations: parent expressions may already be bound to
            // their attributes. DataFrameReader.table analyzes its relation before returning.
            if (!(plan instanceof DataSourceV2Relation)) return plan;
            DataSourceV2Relation relation = (DataSourceV2Relation) plan;
            if (!(relation.table() instanceof CobbleTable)) return plan;
            CobbleOptions.CobbleTableConfig config = ((CobbleTable) relation.table()).config();
            if (!config.isCatalogTable() || config.hasSnapshotId()) return plan;
            String version = relation.options().get(CobbleOptions.SNAPSHOT_ID);
            if (version == null
                    || version.trim().isEmpty()
                    || CobbleOptions.LATEST_SNAPSHOT.equalsIgnoreCase(version.trim())) return plan;

            try {
                Table historical =
                        ((TableCatalog) relation.catalog().get())
                                .loadTable(relation.identifier().get(), version.trim());
                return DataSourceV2Relation.create(
                        historical, relation.catalog(), relation.identifier(), relation.options());
            } catch (NoSuchTableException missing) {
                throw new IllegalArgumentException(
                        "Cobble snapshot table no longer exists.", missing);
            }
        }
    }
}
