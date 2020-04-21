package com.thecookiezen.beam.source;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SslUtils;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import junit.framework.TestCase;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.security.GeneralSecurityException;

@RunWith(JUnit4.class)
public class GcsFileListIOIntegrationTest extends TestCase {

    @Rule
    public GenericContainer gcsServer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withExposedPorts(4443)
            .withClasspathResourceMapping("gcsFiles", "/data/gcsFiles", BindMode.READ_ONLY);

    private final GcsOptions gcsOptions = PipelineOptionsFactory.create().as(GcsOptions.class);

    @Before
    public void setUp() throws GeneralSecurityException {
        String address = "127.0.0.1";
        Integer port = gcsServer.getFirstMappedPort();

        System.out.println(address);
        System.out.println(port);

        ApacheHttpTransport httpTransport = new ApacheHttpTransport(ApacheHttpTransport.newDefaultHttpClientBuilder()
                .setSSLSocketFactory(new SSLConnectionSocketFactory(SslUtils.trustAllSSLContext()))
                .build());

        Storage.Builder storage = new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), new RetryHttpRequestInitializer())
                .setApplicationName(gcsOptions.getAppName())
                .setGoogleClientRequestInitializer(gcsOptions.getGoogleApiTrace())
                .setRootUrl("https://" + address + ":" + port)
                .setSuppressAllChecks(true);

        final GcsUtil gcsUtil = GcsUtil.GcsUtilFactory.create(
                gcsOptions,
                storage.build(),
                storage.getHttpRequestInitializer(),
                gcsOptions.getExecutorService(),
                gcsOptions.getGcsUploadBufferSizeBytes()
        );

        gcsOptions.setGcsUtil(gcsUtil);
    }

    @Test
    public void should_test_integration() throws IOException {
        GcsUtil gcsUtil = gcsOptions.getGcsUtil();

        Objects objects = gcsUtil.listObjects("gcsFiles", "test", null);

        System.out.println(objects.size());
    }
}