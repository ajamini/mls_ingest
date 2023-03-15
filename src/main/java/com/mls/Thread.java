package com.mls;

import com.mls.entity.Property;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Thread implements Runnable {
    static Integer threadCount = 1;
    String media_path;
    String[] headers;
    List<String> nextLines;
    Integer threadNum;

    Thread(String media_path, String[] headers, List<String> nextLines) {
        this.media_path = media_path;
        this.headers = headers;
        this.nextLines = nextLines;
        this.threadNum = Thread.threadCount++;
    }

    public void run() {
        System.out.printf("[Thread %s] starting ingest %s\n", this.threadNum, this.nextLines.size());
        for (String nextLine: this.nextLines) {
            try {
                String row = nextLine;
                Property property = new Property();
                String mls_number = this.get(row, "MLS");

                Integer property_id = property.createProperty(
                    mls_number,
                    this.get(row, "Status"),
                    this.get(row, "StreetName"),
                    this.get(row, "StreetAbbreviation"),
                    this.get(row, "StreetDirection"),
                    this.get(row, "AptUnit"),
                    this.get(row, "Area"),
                    this.get(row, "Province"),
                    this.get(row, "PostalCode"),
                    this.get(row, "Address"),

                    this.get(row, "Community"),
                    this.get(row, "CommunityCode"),
                    this.get(row, "MunicipalityCode"),
                    this.get(row, "MunicipalityDistrict"),
                    this.get(row, "MunicipalityCode"),
                    this.get(row, "DirectionsCrossStreets"),
                    this.get(row, "Zoning"),

                    this.get(row, "RemarksForClients"),
                    this.get(row, "Extras"),
                    this.get(row, "PossessionRemarks"),
                    this.get(row, "LeasedTerms"),

                    this.get(row, "ListPrice"),
                    this.get(row, "thisroxSquareFootage"),
                    this.get(row, "LotDepth"),
                    this.get(row, "LotFront"),
                    this.get(row, "Taxes"),

                    this.getInt(row, "Bedrooms"),
                    this.getInt(row, "BedroomsPlus"),
                    this.getInt(row, "Washrooms"),
                    this.getInt(row, "Kitchens"),
                    this.getInt(row, "KitchensPlus"),
                    this.getInt(row, "Rooms"),
                    this.getInt(row, "RoomsPlus"),

                    "",
                    this.get(row, "ListBrokerage")
                );

                File f = new File(media_path);
                File[] matchingFiles = f.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return name.startsWith("Photo" + mls_number);
                    }
                });

                String uuid = UUID.randomUUID().toString();
                assert matchingFiles != null;
                for (File file : matchingFiles) {
                    String seq = file.getName().split("\\.")[0].split("\\-")[1];
                    String url = this.upload(uuid, Integer.parseInt(seq), file);
                    property.addImage(property_id, null, null, file.getName(), url);
                    System.out.printf("[Thread %s] uploading %s for %s listing\n", this.threadNum, file.getName(), mls_number);
                }

                System.out.printf("[Thread %s] %s inserted with %s images\n", this.threadNum, mls_number, matchingFiles.length);
            } catch (SQLException | IOException e) {
                System.out.println(e);
            }
        }
    }

    public String get(String row, String column) {
        for (int i = 0; i < this.headers.length; i++) {
            if (this.headers[i].equals(column)) {
                return row.split("\t")[i];
            }
        }

        return "";
    }
    public Integer getInt(String row, String column) {
        for (int i = 0; i < this.headers.length; i++) {
            if (this.headers[i].equals(column)) {
                String[] items = row.split("\t");
                if (!items[i].isEmpty()) {
                    return Integer.parseInt(items[i]);
                }
            }
        }

        return 0;
    }

    public String upload(String uuid, Integer sequence, File file) throws IOException {
        Region region = Region.US_WEST_2;
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                "DO00249UPETDJGRQG879", "AXYpFMNUJVZybGv4BUVxznmmOuYxR1lnDojF2eDMA78", ""
        );
        S3Client s3 = S3Client.builder()
                .region(region)
                .endpointOverride(URI.create("https://nyc3.digitaloceanspaces.com"))
                .credentialsProvider(StaticCredentialsProvider.create(sessionCredentials))
                .build();

        //AXYpFMNUJVZybGv4BUVxznmmOuYxR1lnDojF2eDMA78
        //DO00249UPETDJGRQG879
        String key = "listing/photos/" + uuid + "/" + sequence;
        PutObjectRequest request = PutObjectRequest.builder()
                .acl("public-read")
                .bucket("myassist")
                .contentType(Files.probeContentType(file.toPath()))
                .contentDisposition(file.getName())
                .key(key)
                .build();

        s3.putObject(request, file.toPath());

        return "https://myassist.nyc3.digitaloceanspaces.com/" + key;
    }
}
