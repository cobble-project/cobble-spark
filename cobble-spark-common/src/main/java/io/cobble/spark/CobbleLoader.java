package io.cobble.spark;

import io.cobble.NativeLoader;

/** Loads the Cobble native library once per JVM; safe to call on driver and executors. */
public final class CobbleLoader {

    private CobbleLoader() {}

    public static void ensureCobbleLoaded() {
        NativeLoader.load();
    }
}
