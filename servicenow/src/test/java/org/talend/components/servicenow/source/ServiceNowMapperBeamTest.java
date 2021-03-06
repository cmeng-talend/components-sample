package org.talend.components.servicenow.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.talend.components.servicenow.ServiceNow.API_URL;
import static org.talend.components.servicenow.ServiceNow.PASSWORD;
import static org.talend.components.servicenow.ServiceNow.USER;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import avro.shaded.com.google.common.collect.Iterables;

import java.io.Serializable;

import javax.json.JsonObject;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.talend.components.servicenow.configuration.BasicAuthConfig;
import org.talend.components.servicenow.configuration.CommonConfig;
import org.talend.components.servicenow.configuration.TableDataSet;
import org.talend.sdk.component.junit.RecordAsserts;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.http.junit4.JUnit4HttpApi;
import org.talend.sdk.component.junit.http.junit4.JUnit4HttpApiPerMethodConfigurator;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;

public class ServiceNowMapperBeamTest implements Serializable {

    @ClassRule
    public transient static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule(
            "org.talend.components.servicenow");

    @ClassRule
    public transient static final JUnit4HttpApi API = new JUnit4HttpApi().activeSsl();

    //    static {
    //        System.setProperty("talend.junit.http.capture", "true");
    //    }

    @Rule
    public transient final TestPipeline pipeline = TestPipeline.create();

    @Rule
    public transient final JUnit4HttpApiPerMethodConfigurator configurator =
            new JUnit4HttpApiPerMethodConfigurator(API);

    @Test
    public void getRecords() {
        final BasicAuthConfig dataStore = new BasicAuthConfig(API_URL, USER, PASSWORD);

        final TableDataSet configuration = new TableDataSet();
        configuration.setDataStore(dataStore);
        final CommonConfig apiConfig = new CommonConfig();
        apiConfig.setTableName(CommonConfig.Tables.incident);
        configuration.setCommonConfig(apiConfig);
        configuration.setMaxRecords(10);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.asManager()
                .findMapper("ServiceNow", "ServiceNowInput", 1,
                        configurationByExample(configuration, "tableDataSet"))
                .orElseThrow(() -> new RuntimeException("fail"));

        // create a pipeline starting with the mapper
        final PCollection<JsonObject> out = pipeline.apply(TalendIO.read(mapper));

        PAssert.that(out)
                .satisfies(new SimpleFunction<Iterable<JsonObject>, Void>() {

                    @Override public Void apply(final Iterable<JsonObject> input) {
                        assertEquals(10, Iterables.size(input));
                        return null;
                    }
                })
                .satisfies(new SimpleFunction<Iterable<JsonObject>, Void>() {

                    @Override
                    public Void apply(final Iterable<JsonObject> input) {
                        input.forEach((RecordAsserts.SerializableConsumer<JsonObject>) tableRecord -> {
                            assertNotNull(tableRecord.getString("number"));
                        });
                        return null;
                    }
                });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

}
