package org.talend.components.servicenow.output;

import static org.talend.components.servicenow.service.http.TableApiClient.API_BASE;
import static org.talend.components.servicenow.service.http.TableApiClient.API_VERSION;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.talend.components.servicenow.configuration.OutputConfig;
import org.talend.components.servicenow.service.http.TableApiClient;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "ServiceNowTest")
@Processor(name = "ServiceNowTest")
@Documentation("ServiceNowTest is a configurable connector able to write records to Service Now Table")
public class ServiceNowTest implements Serializable {

    private final OutputConfig outputConfig;

    private final JsonBuilderFactory factory;

    TableApiClient client;

    public ServiceNowTest(@Option("configuration") final OutputConfig outputConfig, TableApiClient client,
            JsonBuilderFactory factory) {
        this.outputConfig = outputConfig;
        this.client = client;
        this.factory = factory;
    }

    @PostConstruct
    public void init() {
        client.base(outputConfig.getDataStore().getUrlWithSlashEnding() + API_BASE + "/" + API_VERSION);
    }

    @ElementListener
    public void onNext(@Input("param0") final JsonObject record0, @Input("param1") final JsonObject record1, @Input("param2") final JsonObject record2,
            final @Output("output0") OutputEmitter<JsonObject> output0,
            final @Output("output1") OutputEmitter<JsonObject> output1,
            final @Output("output2") OutputEmitter<JsonObject> output2) {
        output0.emit(record0);
        output1.emit(record1);
        output2.emit(record2);
    }
}