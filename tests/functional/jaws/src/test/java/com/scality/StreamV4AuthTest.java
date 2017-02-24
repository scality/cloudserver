package com.scality;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.io.RandomAccessFile;
import java.security.SecureRandom;

import com.amazonaws.SDKGlobalConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;
import java.io.File;
import java.io.FileReader;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


@RunWith(Parameterized.class)
public class StreamV4AuthTest {
    private Integer fileSize;

    protected static String accessKey;
    public static  String getAccessKey() { return accessKey; }
    protected static String secretKey;
    public static String getSecretKey() { return secretKey; }
    protected static String transport;
    public static String getTransport() { return transport; }
    protected static String ipAddress;
    public static String getIpAddress() { return ipAddress; }
    protected static AmazonS3 s3client;
    public static AmazonS3 getS3Client() { return s3client; }
    public static final String bucketName = "streambucket";
    public static final String objName = "streamObject";
    public static final String fileName = "obj.txt";

    @BeforeClass
    public static void initialize() throws Exception {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        JSONParser parser = new JSONParser();
        String path = Paths.get("../config.json").toAbsolutePath().toString();
        JSONObject obj = (JSONObject) parser.parse(new FileReader(path));
        StreamV4AuthTest.accessKey = (String) obj.get("accessKey");
        StreamV4AuthTest.secretKey = (String) obj.get("secretKey");
        StreamV4AuthTest.transport = (String) obj.get("transport");
        StreamV4AuthTest.ipAddress = (String) obj.get("ipAddress");

        BasicAWSCredentials awsCreds =
            new BasicAWSCredentials(getAccessKey(), getSecretKey());
        s3client = new AmazonS3Client(awsCreds);
        s3client.setEndpoint(getTransport() + "://" + getIpAddress() +
            ":8000");
        s3client.setS3ClientOptions(new S3ClientOptions()
            .withPathStyleAccess(true));
        getS3Client().createBucket(bucketName);
    }

    @AfterClass
    public static void deleteBucket() {
        getS3Client().deleteBucket(bucketName);
    }

    @After
    public void deleteObjectAndFile() throws Exception {
        Path path = Paths.get(fileName).toAbsolutePath();
        Files.delete(path);
        getS3Client().deleteObject(bucketName, objName);
    }

    // Each parameter should be placed as an argument here
    // Every time runner triggers, it will pass the arguments
    // from parameters we defined in primeNumbers() method

    public StreamV4AuthTest(Integer fileSize) {
        this.fileSize = fileSize;
    }

    @Parameterized.Parameters
    public static Collection fileSizes() {
        return Arrays.asList(new Object[][] {
            { 1 },
            { 10 },
            { 50 },
            { 100 },
            // 1 kb
            { 1024 },
            // 1 mb
            { 1048576 },
            // 1.5 mb
            { 1572864 },
            // 2 mb
            { 2097152 },
            // 5 mb
            { 5242880 },
            // 50 mb
            { 52428800 },
            // 100 mb
            { 104857600 },
            // 500 mb
            { 524288000 },
            // 1 gb
            { 1073741824 }

        });
    }

    @Test
    public void testStreamV4Auth() throws Exception {
        System.out.println("Object size is : " + fileSize);
        File sample = createSampleFile(fileSize);
        FileInputStream fis = new FileInputStream(sample);
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        fis.close();
        PutObjectRequest putObjectReq = new PutObjectRequest(bucketName,
            objName, sample);
        putObjectReq.putCustomRequestHeader("Expect", "100-continue");
        getS3Client().putObject(putObjectReq);
        S3Object object = getS3Client()
            .getObject(new GetObjectRequest(bucketName, objName));
        Assert.assertEquals(object.getObjectMetadata().getETag(), md5);
    }

    /**
    * Creates a temporary file
    * @param {Integer} fileSize - file size in bytes
    * @return A newly created temporary file
    * @throws Exception
    */
    private static File createSampleFile(Integer fileSize) throws Exception {
        String alph = "\r\nABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        SecureRandom random = new SecureRandom();
        Integer randomLimit = Math.min(Math.round(fileSize/10), 10000);
        StringBuilder myStringBuilder = new StringBuilder(randomLimit);
        for(int i = 0; i < randomLimit; i++)
              myStringBuilder.append(alph.charAt(random.nextInt(alph.length())));
        String myString = myStringBuilder.toString();
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        file.writeUTF("\r\nlet's add some \r\n data with \r\n\rn\rn\rn\r\n\r\n\n");
        file.writeUTF(myString);
        file.writeUTF("\r\nadd\r\nmore\r\nlines\r\n");
        file.setLength(fileSize);
        file.close();
        File myFile = new File(fileName);
        return myFile;
    }
}
