package com.scality;

import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass ;
import com.amazonaws.SDKGlobalConfiguration;
import java.io.FileReader;
import java.nio.file.Paths;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;


public class JavaTest {
    protected static String accessKey;
    public static  String getAccessKey() { return accessKey; }
    protected static String secretKey;
    public static String getSecretKey() { return secretKey; }
    protected static String transport;
    public static String getTransport() { return transport; }
    protected static String ipAddress;
    public static String getIpAddress() { return ipAddress; }
    protected static AmazonS3 s3client;
    public AmazonS3 getS3Client() { return this.s3client; }
    public static final String bucketName = "somebucket";

    //run once before all the tests
    @BeforeClass public static void initConfig() throws Exception {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        JSONParser parser = new JSONParser();
        String path = Paths.get("../config.json").toAbsolutePath().toString();
        JSONObject obj = (JSONObject) parser.parse(new FileReader(path));
        JavaTest.accessKey = (String) obj.get("accessKey");
        JavaTest.secretKey = (String) obj.get("secretKey");
        JavaTest.transport = (String) obj.get("transport");
        JavaTest.ipAddress = (String) obj.get("ipAddress");

        BasicAWSCredentials awsCreds =
            new BasicAWSCredentials(getAccessKey(), getSecretKey());
        s3client = new AmazonS3Client(awsCreds);
        s3client.setEndpoint(getTransport() + "://" + getIpAddress() +
            ":8000");
        s3client.setS3ClientOptions(new S3ClientOptions()
            .withPathStyleAccess(true));
    }

    @Test public void testCreateBucket() throws Exception {
        getS3Client().createBucket(bucketName);
        Object[] buckets=getS3Client().listBuckets().toArray();
        Assert.assertEquals(buckets.length,1);
        Bucket bucket = (Bucket) buckets[0];
        Assert.assertEquals(bucketName, bucket.getName());
        getS3Client().deleteBucket(bucketName);
        Object[] bucketsAfter=getS3Client().listBuckets().toArray();
        Assert.assertEquals(bucketsAfter.length, 0);
    }

}
