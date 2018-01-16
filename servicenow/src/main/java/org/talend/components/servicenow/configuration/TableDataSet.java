package org.talend.components.servicenow.configuration;

import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Discoverable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@DataSet("table")
@Discoverable("guessTableSchema")
@GridLayout({
        @GridLayout.Row({ "dataStore" }),
        @GridLayout.Row({ "commonConfig" }),
        @GridLayout.Row({ "queryBuilder" }),
        @GridLayout.Row({ "ordered" }),
        @GridLayout.Row({ "order" })
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row({ "limit" }),
        @GridLayout.Row({ "maxRecords" }),
        @GridLayout.Row({ "commonConfig" })
})
@Documentation("This data set represent a Service Now Table, like incident, problem, service...")
public class TableDataSet implements Serializable {

    public static final int READ_ALL_RECORD_FROM_SERVER = -1;

    public static final int MAX_LIMIT = 10000;

    @Option
    private BasicAuthConfig dataStore;

    @Option
    private CommonConfig commonConfig;

    @Option
    @Documentation("Query builder")
    private List<QueryBuilder> queryBuilder;

    @Option
    @Documentation("Order of the data set.")
    private boolean ordered = false;

    @Option
    @ActiveIf(target = "ordered", value = { "true" })
    @Documentation("the data set fields order")
    private List<OrderBuilder> order;

    /**
     * data source start
     */
    private int offset;

    @Option
    @Documentation("Max record to retrieve. Default if -1, set to -1 to get all the data from service now server.")
    private int maxRecords = READ_ALL_RECORD_FROM_SERVER;

    @Option
    @Documentation("limit for pagination. The default is 10000.")
    private int limit = MAX_LIMIT;

    public TableDataSet(TableDataSet mDataSet) {
        this.dataStore = mDataSet.getDataStore();
        this.commonConfig = mDataSet.getCommonConfig();
        this.ordered = mDataSet.isOrdered();
        this.order = mDataSet.getOrder();
        this.queryBuilder = mDataSet.getQueryBuilder();
        this.offset = mDataSet.getOffset();
        this.maxRecords = mDataSet.getMaxRecords();
        this.limit = mDataSet.getLimit();
    }

    public String buildQuery() {
        String query = "";
        if (getQueryBuilder() != null && !getQueryBuilder().isEmpty()) {
            query = getQueryBuilder().stream().map(f -> f.getField().name()
                    + f.getOperation().operation()
                    + f.getValue()).collect(joining("^"));
        }

        if (isOrdered() && getOrder() != null && !getOrder().isEmpty()) {
            String order = getOrder().stream()
                    .map(o -> "ORDERBY" + o.getField().name())
                    .collect(joining("^"));

            query += "^" + order;
        }

        return query.isEmpty() ? null : query;
    }

    /**
     * @return the total record that can be read from this data set
     */
    public int getPageSize() {
        return maxRecords == READ_ALL_RECORD_FROM_SERVER ? limit : Math.min(limit, maxRecords - offset);
    }

}