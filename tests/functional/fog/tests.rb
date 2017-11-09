require "fog/aws"
require "digest/md5"
require "json"
require "securerandom"
require "excon"

config = File.read("../config.json")
rubyConfig = JSON.parse(config)

transport = rubyConfig["transport"];
ipAddress = rubyConfig["ipAddress"];
endpoint = "#{transport}://#{ipAddress}:8000";

if(rubyConfig["certPath"])
    Excon.defaults[:ssl_ca_path] = rubyConfig["caCertPath"]
end

# Disabling to allow for end to end tests in CI
Excon.defaults[:ssl_verify_peer] = false

connection = Fog::Storage.new(
	{   :provider => "AWS",
		:aws_access_key_id => rubyConfig["accessKey"],
		:aws_secret_access_key => rubyConfig["secretKey"],
		:endpoint => endpoint,
  	    :path_style => true,
        :scheme => transport,
	})

ONEMEGABYTE = 1024 * 1024
SIZE = 10


describe Fog do
    fileToStream = "fileToStream.txt"
    downloadFile = "downloadedfile.txt"
    smallFile = "smallFile.txt"
    before(:all) do
        File.open(fileToStream, "wb") do |f|
          SIZE.to_i.times { f.write(
              SecureRandom.random_bytes( ONEMEGABYTE )
              ) }
        end
        $fileToStreamMd5 = Digest::MD5.file(fileToStream).hexdigest

        File.open(smallFile, "wb") do |f|
            f.write('\r\n\r\nsmallfile\r\nwith\r\nline\r\n' +
            'breaks\r\neverywhere\r\n\r\n\r\n')
        end
        $smallFileMd5 = Digest::MD5.file(smallFile).hexdigest
    end

    after(:all) do
        File.unlink(fileToStream)
        File.unlink(smallFile)
        File.unlink(downloadFile)
    end

    $bucketName = "myrubybucket"

    it "should create a bucket" do
        $bucket = connection.directories.create(
          :key => $bucketName
        )
    end

    it "should put a non-streaming object (regular v4 auth)" do
        helloBody = "Hello Fog!"
        non_streamFile = $bucket.files.create(
            :body => helloBody,
            :key  => "helloFog"
        )
        bodyHash = Digest::MD5.hexdigest(helloBody)
        expect(non_streamFile.etag).to eq(bodyHash)
    end

    it "should get a non-streaming object (regular v4 auth)" do
        received = connection.get_object($bucketName, "helloFog")
        expect(received.body).to eq("Hello Fog!")
    end

    it "should delete a non-streaming object (regular v4 auth)" do
        connection.delete_object($bucketName, "helloFog")
    end

    it "should put a streaming object (streaming v4 auth)" do
        streamObject = $bucket.files.create(
            :key => "streamObject",
            :body => File.open(fileToStream),
            :content_type => "text/plain",
            :acl => "private")
        expect(streamObject.etag).to eq($fileToStreamMd5)
    end

    it "should get a streamed object (regular v4 auth)" do
        open(downloadFile, "wb") do |f|
          $bucket.files.get("streamObject") do
            |chunk,remainingBytes,totalBytes|
            f.write chunk
          end
      end
        downloadedFileMd5 = Digest::MD5.file(downloadFile).hexdigest
        expect(downloadedFileMd5).to eq($fileToStreamMd5)
    end

    it "should delete a streamed object (regular v4 auth)" do
        connection.delete_object($bucketName, "streamObject")
    end

    it "should put a small streaming object (streaming v4 auth)" do
        streamObject = $bucket.files.create(
            :key => "smallStream",
            :body => File.open(smallFile))
        expect(streamObject.etag).to eq($smallFileMd5)
    end

    it "should get a small streamed object (regular v4 auth)" do
        open(downloadFile, "wb") do |f|
          $bucket.files.get("smallStream") do
            |chunk,remainingBytes,totalBytes|
            f.write chunk
          end
      end
        downloadedFileMd5 = Digest::MD5.file(downloadFile).hexdigest
        expect(downloadedFileMd5).to eq($smallFileMd5)
    end

    it "should delete a small streamed object (regular v4 auth)" do
        connection.delete_object($bucketName, "smallStream")
    end

    it "should initiate a multipart upload (regular v4 auth)" do
        response = connection.initiate_multipart_upload(
            $bucketName, "mpuObject"
        )
        $uploadId = response.body["UploadId"]
    end

    it "should upload a streaming part (streaming v4 auth)" do
        response = connection.upload_part($bucketName,
            "mpuObject", $uploadId, 1, File.open(fileToStream)
        )
        expect(response.headers["ETag"]).to eq("\"#{$fileToStreamMd5}\"")
    end

    it "should upload a small streaming part (streaming v4 auth)" do
        response = connection.upload_part($bucketName,
            "mpuObject", $uploadId, 2, File.open(smallFile)
        )
        expect(response.headers["ETag"]).to eq("\"#{$smallFileMd5}\"")
    end

    it "should abort a multipart upload (regular v4 auth)" do
        connection.abort_multipart_upload(
            $bucketName, "mpuObject", $uploadId
        )
    end

    it "should put a streaming object to AWS (streaming v4 auth)", :skip => true do
        streamObject = $bucket.files.create(
            :key => "awsStreamObject",
            :body => File.open(fileToStream),
            :metadata => { "x-amz-meta-scal-location-constraint":"awsbackend" },
            :content_type => "text/plain",
            :acl => "private")
        expect(streamObject.etag).to eq($fileToStreamMd5)
    end

    it "should get a streamed object from AWS (regular v4 auth)", :skip => true do
        open(downloadFile, "wb") do |f|
            $bucket.files.get("awsStreamObject") do
                |chunk,remainingBytes,totalBytes|
                f.write chunk
            end
        end
        downloadedFileMd5 = Digest::MD5.file(downloadFile).hexdigest
        expect(downloadedFileMd5).to eq($fileToStreamMd5)
    end

    it "should delete a streamed object from AWS (regular v4 auth", :skip => true do
        connection.delete_object($bucketName, "awsStreamObject")
    end

    it "should initiate a multipart upload on AWS (regular v4 auth)", :skip => true do
        response = connection.initiate_multipart_upload(
            $bucketName, "awsMpuObject",
            options = { "x-amz-meta-scal-location-constraint": "awsbackend" },
        )
        $awsUploadId = response.body["UploadId"]
    end

    it "should upload a streaming part on AWS(streaming v4 auth)", :skip => true do
        response = connection.upload_part($bucketName,
            "awsMpuObject", $awsUploadId, 1, File.open(fileToStream)
        )
        expect(response.headers["ETag"]).to eq("\"#{$fileToStreamMd5}\"")
    end

    it "should abort a multipart upload on AWS (regular v4 auth)", :skip => true do
        connection.abort_multipart_upload(
            $bucketName, "awsMpuObject", $awsUploadId
        )
    end

    it "should delete the bucket (regular v4 auth)" do
        $bucket.destroy
    end

end
