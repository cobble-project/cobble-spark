package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.PrivilegedExceptionAction;

class CobbleHadoopContextTest {
    @Test
    void deserializedContextPrefersRenewedSameUserTokensAndExcludesOtherUsersTokens()
            throws Exception {
        Text alias = new Text("storage-token"), unrelated = new Text("other-user-token");
        UserGroupInformation original = UserGroupInformation.createRemoteUser("spark-storage-user");
        original.addToken(alias, token((byte) 1));
        CobbleHadoopContext captured =
                original.doAs(
                        (PrivilegedExceptionAction<CobbleHadoopContext>)
                                () -> CobbleHadoopContext.capture(new Configuration(false)));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(captured);
        }
        CobbleHadoopContext restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (CobbleHadoopContext) input.readObject();
        }
        UserGroupInformation renewed = UserGroupInformation.createRemoteUser("spark-storage-user");
        renewed.addToken(alias, token((byte) 2));
        renewed.doAs(
                (PrivilegedExceptionAction<Void>)
                        () -> {
                            assertArrayEquals(
                                    new byte[] {2},
                                    restored.identity()
                                            .getCredentials()
                                            .getToken(alias)
                                            .getPassword());
                            return null;
                        });
        UserGroupInformation other = UserGroupInformation.createRemoteUser("another-user");
        other.addToken(alias, token((byte) 3));
        other.addToken(unrelated, token((byte) 4));
        other.doAs(
                (PrivilegedExceptionAction<Void>)
                        () -> {
                            UserGroupInformation identity = restored.identity();
                            assertArrayEquals(
                                    new byte[] {1},
                                    identity.getCredentials().getToken(alias).getPassword());
                            assertNull(identity.getCredentials().getToken(unrelated));
                            return null;
                        });
    }

    private static Token<?> token(byte password) {
        return new Token<>(
                new byte[] {0}, new byte[] {password}, new Text("mock"), new Text("storage"));
    }
}
