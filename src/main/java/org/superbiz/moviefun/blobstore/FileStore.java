package org.superbiz.moviefun.blobstore;

import org.apache.tika.Tika;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.lang.ClassLoader.getSystemResource;
import static java.lang.String.format;
import static java.nio.file.Files.readAllBytes;

public class FileStore implements BlobStore {

    @Override
    public void put(Blob blob) throws IOException {
        File targetFile = getCoverFile(blob.name);

        targetFile.delete();
        targetFile.getParentFile().mkdirs();
        targetFile.createNewFile();

        try (FileOutputStream outputStream = new FileOutputStream(targetFile)) {
            outputStream.write(blob.inputStream.read());
        }
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        Path coverFilePath = getExistingCoverPath(name);

        if (coverFilePath == null){
            return Optional.empty();
        }

        byte[] imageBytes = readAllBytes(coverFilePath);
        InputStream inputStream = new ByteArrayInputStream(imageBytes);
        return Optional.of(new Blob(name, inputStream, new Tika().detect(inputStream)));
    }

    @Override
    public void deleteAll() {
        // ...
    }

    private File getCoverFile(String name) {
        return new File(name);
    }

    private Path getExistingCoverPath(String name){
        File coverFile = getCoverFile(name);
        Path coverFilePath = null;

        if (coverFile.exists()) {
            coverFilePath = coverFile.toPath();
        }

        return coverFilePath;
    }
}
