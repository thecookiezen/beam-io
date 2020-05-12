package com.thecookiezen.beam.source;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class GcsFileListIOTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Rule
    public MethodRule agentAttachmentRule = new AgentAttachmentRule();

    private static GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    @Before
    public void setUp() {
        GcsUtilInterceptor.gcsUtil = mockGcsUtil;

        new ByteBuddy()
                .redefine(GcsUtil.GcsUtilFactory.class)
                .method(named("create"))
                .intercept(to(GcsUtilInterceptor.class))
                .make()
                .load(GcsUtil.GcsUtilFactory.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
    }

    @Test
    public void should_stream_file_names_from_the_bucket() throws IOException {
        Objects modelObjects = new Objects();
        List<StorageObject> items = new ArrayList<>();
        // A directory
        items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/"));

        // Files within the directory
        items.add(createStorageObject("gs://testbucket/testdirectory/file1name"));
        items.add(createStorageObject("gs://testbucket/testdirectory/file2name"));
        items.add(createStorageObject("gs://testbucket/testdirectory/file3name"));
        items.add(createStorageObject("gs://testbucket/testdirectory//"));
        items.add(createStorageObject("gs://testbucket/testdirectory/otherfile"));

        modelObjects.setItems(items);

        when(mockGcsUtil.listObjects(anyString(), anyString(), isNull()))
                .thenReturn(modelObjects)
                .thenReturn(new Objects().setItems(Collections.emptyList()));

        final PCollection<String> output =
                pipeline.apply("ReadFileNames", GcsFileListIO.fromPath(GcsPath.fromUri("gs://testbucket/testdirectory/*")));

        PAssert.that(output).containsInAnyOrder("file1name", "file2name", "file3name", "otherfile");

        pipeline.run();
    }

    private StorageObject createStorageObject(String gcsFilename) {
        GcsPath gcsPath = GcsPath.fromUri(gcsFilename);
        return new StorageObject()
                .setBucket(gcsPath.getBucket())
                .setName(gcsPath.getObject())
                .setSize(BigInteger.ZERO);
    }

}