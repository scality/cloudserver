require "fog"
require "digest/md5"
require "json"
require "securerandom"
require "excon"

config = File.read("config.json")
rubyConfig = JSON.parse(config)

transport = rubyConfig["transport"];
ipAddress = ENV["IP"] ? ENV["IP"]: '127.0.0.1'
endpoint = "#{transport}://#{ipAddress}:8000";

if(rubyConfig["certPath"])
    Excon.defaults[:ssl_ca_path] = rubyConfig["certPath"]
end

# Excon.defaults[:ssl_verify_peer] = false
# Excon.defaults[:ssl_uri_schemes] = [ transport ]

connection = Fog::Storage.new(
	{ :provider => "AWS",
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
    before(:all) do
        File.open(fileToStream, "wb") do |f|
          SIZE.to_i.times { f.write(
              SecureRandom.random_bytes( ONEMEGABYTE )
              ) }
        end
        $fileToStreamMd5 = Digest::MD5.file(fileToStream).hexdigest
    end

    after(:all) do
        File.unlink(fileToStream)
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
        bodyHash = "0da6049275b6b9a3c38e729ac139c09f"
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
        expect(streamObject.etag).to eq(fileToStreamMd5)
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
        expect(response.etag).to eq(fileToStreamMd5)
    end

    it "should abort a multipart upload (regular v4 auth)" do
        connection.abort_multipart_upload(
            $bucketName, "mpuObject", $uploadId
        )
    end

    it "should delete the bucket (regular v4 auth)" do
        $bucket.destroy
    end

end
