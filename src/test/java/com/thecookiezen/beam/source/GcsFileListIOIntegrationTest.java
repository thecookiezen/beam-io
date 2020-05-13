package com.thecookiezen.beam.source;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SslUtils;
import com.google.api.services.storage.Storage;
import junit.framework.TestCase;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.security.GeneralSecurityException;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;

@RunWith(JUnit4.class)
public class GcsFileListIOIntegrationTest extends TestCase {

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Rule
    public GenericContainer gcsServer = new GenericContainer<>("fsouza/fake-gcs-server")
            .withExposedPorts(4443)
            .withClasspathResourceMapping("gcs-files", "/data/gcs-files", BindMode.READ_ONLY);

    @Before
    public void setUp() throws GeneralSecurityException {
        String address = "127.0.0.1";
        Integer port = gcsServer.getFirstMappedPort();

        final GcsOptions gcsOptions = PipelineOptionsFactory.create().as(GcsOptions.class);

        ApacheHttpTransport httpTransport = new ApacheHttpTransport(ApacheHttpTransport.newDefaultHttpClientBuilder()
                .setSSLSocketFactory(new SSLConnectionSocketFactory(SslUtils.trustAllSSLContext()))
                .build());

        Storage.Builder storage = new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), new RetryHttpRequestInitializer())
                .setApplicationName(gcsOptions.getAppName())
                .setGoogleClientRequestInitializer(gcsOptions.getGoogleApiTrace())
                .setRootUrl("https://" + address + ":" + port)
                .setSuppressAllChecks(true);

        GcsUtilInterceptor.gcsUtil = GcsUtil.GcsUtilFactory.create(
                gcsOptions,
                storage.build(),
                storage.getHttpRequestInitializer(),
                gcsOptions.getExecutorService(),
                gcsOptions.getGcsUploadBufferSizeBytes()
        );

        new ByteBuddy()
                .redefine(GcsUtil.GcsUtilFactory.class)
                .method(named("create"))
                .intercept(to(GcsUtilInterceptor.class))
                .make()
                .load(GcsUtil.GcsUtilFactory.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
    }

    @Test
    @AgentAttachmentRule.Enforce(redefinesClasses = true)
    public void should_test_integration() {
        PCollection<String> output = pipeline
                .apply(GcsFileListIO.fromPath(GcsPath.fromUri("gs://gcs-files/*")));

        PAssert.that(output).containsInAnyOrder("test1.txt", "test2.txt", "anotherTest.txt");

        pipeline.run();
    }
}