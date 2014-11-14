/*
 * Demo for Amazon S3
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3Sample {

    public static void main(String[] args) throws IOException {
    	
    	String accessKeyId = "your keyId";
    	String secretKey = "your secretKey";

        AWSCredentials credentials = null;
        try {
            //credentials = new ProfileCredentialsProvider("default").getCredentials();
            credentials = new BasicAWSCredentials(accessKeyId, secretKey);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Hoa Mai\\.aws\\credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);

        String bucketNameUS = "lumenaki-s3-bucket-" + UUID.randomUUID();
        String key = "file_Object_Key";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
         	//create a US bucket
            System.out.println("Creating bucket " + bucketNameUS + "\n");
            s3.createBucket(bucketNameUS);
            //A single AWS Account can have a maximum of 100 Buckets so Lumenaki only needs one bucket for all users (maybe setted in US)
            
            /*
            //create a EU bucket
        	String bucketNameEU = "lumnaki-s3-bucket-" + UUID.randomUUID();
            System.out.println("Creating bucket " + bucketNameUS + "\n");
            Region euCentral1 = Region.getRegion(Regions.EU_CENTRAL_1);
            s3.createBucket(bucketNameEU, euCentral1);
            */
            

            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            
            //Set permissions of bucket and bucket's content to public
            //get the ACL
            AccessControlList acl = s3.getBucketAcl(bucketNameUS);
            //give everyone read access
            acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
            s3.setBucketAcl(bucketNameUS, acl);
            
            //put the data on S3
            System.out.println("Uploading a new object to S3 from a file\n");
            s3.putObject(new PutObjectRequest(bucketNameUS, key, createSampleFile()));
            //give everyone read access after put file on S3
            s3.setObjectAcl(bucketNameUS, key, acl);

            // Download an object
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketNameUS, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent()); //Display content of object

            //List objects in your bucket by prefix
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketNameUS)
                    .withPrefix("file_"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");
            }

            //Delete an object
            System.out.println("Deleting an object\n");
            s3.deleteObject(bucketNameUS, key);

            /*
             * Delete a bucket - A bucket must be completely empty before it can be
             * deleted, so remember to delete any objects from your buckets before
             * you try to delete them.
             */
            System.out.println("Deleting bucket " + bucketNameUS + "\n");
            s3.deleteBucket(bucketNameUS);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("file_demo", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("Demo S3 Lumenaki \n");
        writer.write("Pyramid Consulting - Etown1 \n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
