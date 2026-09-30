package io.cobble.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Spark's operation-scoped Hadoop configuration and identity; never passed to native metadata. */
final class CobbleHadoopContext implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, String> configuration;
    private final String user;
    private final byte[] credentials;
    private transient UserGroupInformation driverUser;
    private transient ClassLoader classLoader;

    private CobbleHadoopContext(Configuration configuration, UserGroupInformation user)
            throws IOException {
        Map<String, String> values = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration)
            values.put(entry.getKey(), entry.getValue());
        this.configuration = Collections.unmodifiableMap(values);
        this.user = user.getUserName();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            user.getCredentials().writeTokenStorageToStream(output);
        }
        this.credentials = bytes.toByteArray();
        this.driverUser = user;
        this.classLoader = Thread.currentThread().getContextClassLoader();
    }

    static CobbleHadoopContext capture() {
        scala.Option<SparkSession> session = SparkSession.getActiveSession();
        if (session.isEmpty()) session = SparkSession.getDefaultSession();
        Configuration configuration =
                session.isDefined()
                        ? session.get().sparkContext().hadoopConfiguration()
                        : new Configuration();
        return capture(configuration);
    }

    static CobbleHadoopContext capture(Configuration configuration) {
        try {
            return new CobbleHadoopContext(configuration, UserGroupInformation.getCurrentUser());
        } catch (IOException error) {
            throw new IllegalStateException("Failed to capture Hadoop identity.", error);
        }
    }

    Configuration configuration() {
        Configuration result = new Configuration(false);
        ClassLoader loader =
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        result.setClassLoader(loader == null ? CobbleHadoopContext.class.getClassLoader() : loader);
        for (Map.Entry<String, String> entry : configuration.entrySet())
            result.set(entry.getKey(), entry.getValue());
        return result;
    }

    UserGroupInformation identity() throws IOException {
        if (driverUser != null) return driverUser;
        UserGroupInformation current = UserGroupInformation.getCurrentUser();
        UserGroupInformation identity =
                current.getUserName().equals(user)
                        ? UserGroupInformation.createProxyUser(user, current)
                        : UserGroupInformation.createRemoteUser(user);
        Credentials captured = new Credentials();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(credentials))) {
            captured.readTokenStorageStream(input);
        }
        identity.addCredentials(captured);
        if (current.getUserName().equals(user)) identity.addCredentials(current.getCredentials());
        return identity;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CobbleHadoopContext)) return false;
        CobbleHadoopContext that = (CobbleHadoopContext) other;
        return user.equals(that.user)
                && configuration.equals(that.configuration)
                && Arrays.equals(credentials, that.credentials);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, configuration, Arrays.hashCode(credentials));
    }
}
