package com.thecookiezen.beam.source;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class GcsFileListIO {

    private GcsFileListIO() {}

    public static Read fromPath(GcsPath path) {
        return new Read(path);
    }

    public static class Read extends PTransform<PBegin, PCollection<String>> {
        private final GcsPath path;

        public Read(GcsPath path) {
            this.path = path;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            return input
                    .apply(org.apache.beam.sdk.io.Read.from(new GcsFilesListSource(path)))
                    .apply(Flatten.iterables());
        }
    }

    static class GcsFilesListSource extends BoundedSource<List<String>> {

        private final GcsPath path;

        public GcsFilesListSource(GcsPath path) {
            this.path = path;
        }

        @Override
        public List<? extends BoundedSource<List<String>>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
            return Collections.singletonList(new GcsFileListIO.GcsFilesListSource(path));
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
            return 0L;
        }

        @Override
        public BoundedReader<List<String>> createReader(PipelineOptions options) throws IOException {
            return new GcsFilesListReader(options.as(GcsOptions.class), path, this);
        }
    }

    static class GcsFilesListReader extends BoundedSource.BoundedReader<List<String>> {

        private final GcsOptions storage;
        private final GcsFilesListSource gcsFilesListSource;
        private final String bucket;
        private final String prefix;

        private String pageToken;
        private List<String> result = Collections.emptyList();

        public GcsFilesListReader(GcsOptions storage, GcsPath gcsPattern, GcsFilesListSource gcsFilesListSource) {
            this.storage = storage;
            this.bucket = gcsPattern.getBucket();
            this.prefix = GcsUtil.getNonWildcardPrefix(gcsPattern.getObject());
            this.gcsFilesListSource = gcsFilesListSource;
        }

        @Override
        public boolean start() throws IOException {
            return loadData();
        }

        @Override
        public boolean advance() throws IOException {
            return loadData();
        }

        public boolean loadData() throws IOException {
            Objects objects = storage.getGcsUtil().listObjects(bucket, prefix, pageToken);

            result = objects.getItems()
                    .stream()
                    .map(StorageObject::getSelfLink)
                    .collect(Collectors.toList());

            pageToken = objects.getNextPageToken();
            return !result.isEmpty();
        }

        @Override
        public List<String> getCurrent() throws NoSuchElementException {
            return result;
        }

        @Override
        public void close() {}

        @Override
        public BoundedSource<List<String>> getCurrentSource() {
            return gcsFilesListSource;
        }
    }
}
