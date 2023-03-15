package com.mls;

import com.mls.entity.Property;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.lang3.Range;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.Executors.callable;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, CsvException, SQLException, InterruptedException {
        Integer threadCount = 20;
        Integer bufferSize = 8192*20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        //csv file containing data
        String strFile = args[0];
        Scanner reader = new Scanner(new BufferedReader(new FileReader(strFile), bufferSize));
        reader.useDelimiter("\n");
        String[] headers = reader.next().split("\t");

        //read all lines
        List<String> lines = new ArrayList<>();
        while (reader.hasNext()) {
            lines.add(reader.next());
        }

        Integer chuckSize = lines.size() / threadCount;
        System.out.printf("Total Lines %s | Chuck size %s\n", lines.size(), chuckSize);

        executor.invokeAll(
            IntStream
                .range(0,threadCount)
                .mapToObj(i -> callable(new Thread(args[1], headers, lines.subList(i*chuckSize, Math.min((i+1) * chuckSize, lines.size())))))
                .collect(Collectors.toList())
        );

        executor.shutdown();
    }
}
