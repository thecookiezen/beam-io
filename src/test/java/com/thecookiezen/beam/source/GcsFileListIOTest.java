package com.thecookiezen.beam.source;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

class GcsFileListIOTest {

    private final GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);

    private GcsUtil mockGcsUtil;

    @BeforeAll
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        gcsOptions.setGcsUtil(mockGcsUtil);
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
        items.add(createStorageObject("gs://testbucket/testdirectory/file4name"));
        items.add(createStorageObject("gs://testbucket/testdirectory/otherfile"));
        items.add(createStorageObject("gs://testbucket/testdirectory/anotherfile"));

        modelObjects.setItems(items);

        when(mockGcsUtil.listObjects(eq("testbucket"), anyString(), isNull()))
                .thenReturn(modelObjects);

        Pipeline p = Pipeline.create(gcsOptions);

        p.apply("ReadFileNames", GcsFileListIO.fromPath(GcsPath.fromUri("gs://testbucket/testdirectory/")));

        PipelineResult result = p.run();
        PipelineResult.State state = result.waitUntilFinish();
//        assertThat(state, equalTo(PipelineResult.State.DONE));

    }

    private StorageObject createStorageObject(String gcsFilename) {
        GcsPath gcsPath = GcsPath.fromUri(gcsFilename);
        return new StorageObject()
                .setBucket(gcsPath.getBucket())
                .setName(gcsPath.getObject())
                .setSize(BigInteger.ZERO);
    }

}