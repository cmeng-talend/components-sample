/**
 * Copyright (C) 2006-2017 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.components.widget.source;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;

import lombok.Data;

@DataSet("Dataset")
@GridLayout({
    @GridLayout.Row({ "connection", "passwordConfirmation" }),
    @GridLayout.Row({ "dataSetName" }),
    @GridLayout.Row({ "letter" })
})
@Data
public class DatasetConfiguration implements Serializable {

    @Option
    private ConnectionConfiguration connection;
    
    @Option
    @Credential
    private String passwordConfirmation;
    
    @Option
    @Required
    @Min(3)
    @Max(12)
    @Validable(value = "tableValidation")
    private String dataSetName = "my dataset";

    @Option
    @Proposable("sample")
    private String letter = "a";
}
