package main

import (
    "fmt"
    "time"
    "bytes"
    "net/http"
    "net/url"
    "io/ioutil"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/signer/v4"
)

func main() {
    // Input AWS access key, secret key
    aws_access_key_id := "accessKey1"
    aws_secret_access_key := "verySecretKey1"
    endpoint := "http://localhost:8000"
    bucket_name := "bucketname"
    searchQuery := url.QueryEscape("x-amz-meta-color=blue")
    buf := bytes.NewBuffer([]byte{})

    requestUrl := fmt.Sprintf("%s/%s?search=%s",
        endpoint, bucket_name, searchQuery)

    request, err := http.NewRequest("GET", requestUrl, buf)
    if err != nil {
        panic(err)
    }
    reader := bytes.NewReader(buf.Bytes())
    credentials := credentials.NewStaticCredentials(aws_access_key_id,
        aws_secret_access_key, "")
    signer := v4.NewSigner(credentials)
    signer.Sign(request, reader, "s3", "us-east-1", time.Now())
    client := &http.Client{}
    resp, err := client.Do(request)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(body))
}
