package org.superbiz.moviefun.blobstore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.tika.Tika;
import org.apache.tika.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

public class S3Store implements BlobStore {

    private final AmazonS3Client s3Client;
    private final String photoStorageBucket;
    private final Tika tika = new Tika();

    public S3Store(AmazonS3Client s3Client, String photoStorageBucket) {
        this.s3Client = s3Client;
        this.photoStorageBucket = photoStorageBucket;
    }

    @Override
    public void put(Blob blob) throws IOException {

        PutObjectRequest request = new PutObjectRequest(photoStorageBucket, blob.name, blob.inputStream, new ObjectMetadata());
        s3Client.putObject(request);
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        S3Object fullObject = null;
        byte[] bytes;
        try {
            fullObject = s3Client.getObject(new GetObjectRequest(photoStorageBucket, name));
            bytes = IOUtils.toByteArray(fullObject.getObjectContent());
        } catch (SdkClientException ex){
            return Optional.empty();
        }
        return Optional.of(new Blob(name, new ByteArrayInputStream(bytes), tika.detect(bytes)));
    }

    @Override
    public void deleteAll() {

    }
}
