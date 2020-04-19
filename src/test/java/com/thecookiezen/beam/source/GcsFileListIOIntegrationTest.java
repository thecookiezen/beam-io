package com.thecookiezen.beam.source;

import com.google.api.services.storage.Storage;
import junit.framework.TestCase;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

/**
 * curl --insecure https://0.0.0.0:4443/storage/v1/b
 */
@RunWith(JUnit4.class)
public class GcsFileListIOIntegrationTest extends TestCase {

    @Rule
    public GenericContainer gcsServer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withExposedPorts(4443)
            .withClasspathResourceMapping("gcsFiles", "/data", BindMode.READ_ONLY);

    private final GcsOptions gcsOptions = PipelineOptionsFactory.create().as(GcsOptions.class);

    @Before
    public void setUp() {
        String address = gcsServer.getContainerIpAddress();
        Integer port = gcsServer.getFirstMappedPort();

        System.out.println(address);
        System.out.println(port);

        final Storage.Builder storage = Transport
                .newStorageClient(gcsOptions)
                .setRootUrl(address + ":" + port)
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
    public void should_test_integration() {
        //todo
    }

}