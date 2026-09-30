package io.cobble.spark;

import io.cobble.Config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Native provider options carried with each table operation, including portable plans. */
final class CobbleStorageOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String PREFIX = "storage.option.";
    private final Map<String, String> options;

    private CobbleStorageOptions(Map<String, String> options) {
        this.options = Collections.unmodifiableMap(options);
    }

    static boolean isStorageOption(String key) {
        return key.toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    static CobbleStorageOptions parse(Map<String, String> tableOptions) {
        Map<String, String> options = new HashMap<>();
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            if (!isStorageOption(entry.getKey())) continue;
            String key = entry.getKey().substring(PREFIX.length());
            if (key.isEmpty() || entry.getValue() == null) {
                throw new IllegalArgumentException(
                        "storage.option.<name> requires a name and value.");
            }
            String canonical = key.toLowerCase(Locale.ROOT);
            if (isCredentialKey(canonical)) key = canonical;
            else if (canonical.contains("secret")
                    || canonical.contains("password")
                    || canonical.contains("credential")
                    || canonical.contains("token")
                    || canonical.contains("access_key")
                    || canonical.contains("access.key")
                    || canonical.contains("api_key")
                    || canonical.contains("private_key")) {
                throw new IllegalArgumentException(
                        "Sensitive provider options must use supported S3 credential keys; configure Hadoop credentials through spark.hadoop settings.");
            }
            String previous = options.put(key, entry.getValue());
            if (previous != null && !previous.equals(entry.getValue())) {
                throw new IllegalArgumentException("Conflicting provider storage options.");
            }
        }
        return new CobbleStorageOptions(options);
    }

    void applyTo(Config.VolumeDescriptor volume) {
        volume.customOptions = new HashMap<>(options);
        volume.accessId = options.get("access_key_id");
        volume.secretKey = options.get("secret_access_key");
    }

    // These are the credential names Cobble removes from persisted volume descriptors.
    private static boolean isCredentialKey(String key) {
        switch (key) {
            case "access_id":
            case "access_key":
            case "access_key_id":
            case "aws_access_key_id":
            case "secret_key":
            case "secret_access_key":
            case "aws_secret_access_key":
            case "session_token":
            case "aws_session_token":
                return true;
            default:
                return false;
        }
    }
}
