package com.thecookiezen.beam.source;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class GcsFileListIOTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    class TestRead extends PTransform<PBegin, PCollection<String>> {
        private final GcsPath path;

        public TestRead(GcsPath path) {
            this.path = path;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            return input
                    .apply(org.apache.beam.sdk.io.Read.from(new TestGcsFilesListSource(path)))
                    .apply(Flatten.iterables())
                    .apply("Reshuffle", Reshuffle.viaRandomKey());
        }
    }

    static class TestGcsFilesListSource extends BoundedSource<List<String>> {

        private final GcsPath path;

        public TestGcsFilesListSource(GcsPath path) {
            this.path = path;
        }

        @Override
        public List<? extends BoundedSource<List<String>>> split(long desiredBundleSizeBytes, PipelineOptions options) {
            return Collections.singletonList(new TestGcsFilesListSource(path));
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) {
            return 0L;
        }

        @Override
        public BoundedReader<List<String>> createReader(PipelineOptions options) throws IOException {
            options.as(GcsOptions.class).setGcsUtil(mockGcsUtil);
            return new GcsFileListIO.GcsFilesListReader(options.as(GcsOptions.class), path, null);
        }

        @Override
        public Coder<List<String>> getOutputCoder() {
            return ListCoder.of(StringUtf8Coder.of());
        }
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
                pipeline.apply("ReadFileNames", new TestRead(GcsPath.fromUri("gs://testbucket/testdirectory/*")));

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