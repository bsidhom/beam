package org.apache.beam.runners.flink.execution;


import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.flink.FlinkCachedArtifactPaths;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.flink.api.common.cache.DistributedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link org.apache.beam.runners.fnexecution.artifact.ArtifactSource} that draws artifacts
 * from the Flink Distributed File Cache {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class FlinkArtifactSource implements ArtifactSource {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkArtifactSource.class);
  private static final int DEFAULT_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;

  public static FlinkArtifactSource createDefault(DistributedCache cache) {
    return new FlinkArtifactSource(cache, FlinkCachedArtifactPaths.createDefault());
  }

  public static FlinkArtifactSource forToken(DistributedCache cache, String artifactToken) {
    return new FlinkArtifactSource(cache, FlinkCachedArtifactPaths.forToken(artifactToken));
  }

  private final DistributedCache cache;
  private final FlinkCachedArtifactPaths paths;

  private FlinkArtifactSource(DistributedCache cache, FlinkCachedArtifactPaths paths) {
    this.cache = cache;
    this.paths = paths;
  }

  @Override
  public Manifest getManifest() throws IOException {
    String path = paths.getManifestPath();
    LOG.debug("Retrieving manifest {}.", path);
    File manifest;
    try {
      // cache.geFile throws an IllegalArgumentException for unrecognized paths
      manifest = cache.getFile(path);
    } catch (IllegalArgumentException e) {
      return Manifest.getDefaultInstance();
    }
    try (BufferedInputStream fStream = new BufferedInputStream(new FileInputStream(manifest))) {
      return Manifest.parseFrom(fStream);
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    String path = paths.getArtifactPath(name);
    LOG.debug("Retrieving artifact {}.", path);
    try {
      // cache.geFile throws an IllegalArgumentException for unrecognized paths
      File artifact = cache.getFile(path);
      try (FileInputStream fStream = new FileInputStream(artifact)) {
        byte[] buffer = new byte[DEFAULT_CHUNK_SIZE_BYTES];
        for (int br = fStream.read(buffer); br > 0; br = fStream.read(buffer)) {
          ByteString data = ByteString.copyFrom(buffer, 0, br);
          responseObserver.onNext(ArtifactChunk.newBuilder().setData(data).build());
        }
        responseObserver.onCompleted();
      }
    } catch (FileNotFoundException | IllegalArgumentException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", name))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", name))
              .withCause(e)
              .asException());
    }
  }
}
