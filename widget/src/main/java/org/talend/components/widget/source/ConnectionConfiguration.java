package org.talend.components.widget.source;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;

@GridLayout({
    @GridLayout.Row({ "url" }),
    @GridLayout.Row({ "user", "password" }),
})
@DataStore("Connection")
@Checkable("testConnection")
public class ConnectionConfiguration implements Serializable {

    @Option
    @Pattern("^(http|https|ftp).*")
    private String url;

    @Option
    private String user;

    @Option
    @Credential
    @Required
    @Validable(value = "pwdConfirm", parameters = {".", "../passwordConfirmation"})
    private String password;

    public String getURL() {
        return url;
    }

    public ConnectionConfiguration setURL(final String URL) {
        this.url = URL;
        return this;
    }

    public String getUser() {
        return user;
    }

    public ConnectionConfiguration setUser(final String User) {
        this.user = User;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ConnectionConfiguration setPassword(final String Password) {
        this.password = Password;
        return this;
    }
}