package com.thecookiezen.beam.source;

import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;

public class GcsUtilInterceptor {
    static GcsUtil gcsUtil;

    public static GcsUtil inject() {
        return gcsUtil;
    }
}
