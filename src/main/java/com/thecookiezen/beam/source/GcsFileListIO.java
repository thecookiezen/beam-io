package com.thecookiezen.beam.source;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
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
                    .apply(Flatten.iterables())
                    .apply("Reshuffle", Reshuffle.viaRandomKey());
        }
    }

    static class GcsFilesListSource extends BoundedSource<List<String>> {

        private final GcsPath path;

        public GcsFilesListSource(GcsPath path) {
            this.path = path;
        }

        @Override
        public List<? extends BoundedSource<List<String>>> split(long desiredBundleSizeBytes, PipelineOptions options) {
            return Collections.singletonList(new GcsFileListIO.GcsFilesListSource(path));
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) {
            return 0L;
        }

        @Override
        public BoundedReader<List<String>> createReader(PipelineOptions options) throws IOException {
            return new GcsFilesListReader(options.as(GcsOptions.class), path, this);
        }

        @Override
        public Coder<List<String>> getOutputCoder() {
            return ListCoder.of(StringUtf8Coder.of());
        }
    }

    static class GcsFilesListReader extends BoundedSource.BoundedReader<List<String>> {

        private final GcsUtil storage;
        private final GcsPath gcsPath;
        private final GcsFilesListSource gcsFilesListSource;
        private final String prefix;
        private final Pattern pattern;

        private String pageToken;
        private List<String> result = Collections.emptyList();

        public GcsFilesListReader(GcsOptions storage, GcsPath gcsPath, GcsFilesListSource gcsFilesListSource) throws IOException {
            this.storage = storage.getGcsUtil();
            this.gcsPath = gcsPath;
            this.gcsFilesListSource = gcsFilesListSource;
            this.prefix = GcsUtil.getNonWildcardPrefix(gcsPath.getObject());
            this.pattern = Pattern.compile(GcsUtil.wildcardToRegexp(gcsPath.getObject()));
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
            Objects objects = storage.listObjects(gcsPath.getBucket(), prefix, pageToken);

            result = objects.getItems()
                    .stream()
                    .map(StorageObject::getName)
                    .filter(file -> pattern.matcher(file).matches() && !file.endsWith("/"))
                    .map(name -> {
                        final String[] split = name.split("/");
                        return split[split.length - 1];
                    })
                    .collect(Collectors.toList());

            pageToken = objects.getNextPageToken();
            return !result.isEmpty();
        }

        @Override
        public List<String> getCurrent() throws NoSuchElementException {
            return result;
        }

        @Override
        public void close() {
        }

        @Override
        public BoundedSource<List<String>> getCurrentSource() {
            return gcsFilesListSource;
        }
    }
}
