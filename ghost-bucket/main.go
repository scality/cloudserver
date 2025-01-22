package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

/*
Dirty script to generate ghost buckets. It performs a lot of CreateBucket and DeleteBucket operations in parallel, for a
large number of buckets. After a while, it checks if the buckets are ghost buckets and prints a list. It then tries to
recreate the ghost buckets and checks again. It prints a list of deep ghost buckets which are the buckets that remain in
the ghost state after trying to recreate them.

Exanple usage:
AWS_ACCESS_KEY_ID=accessKey1 AWS_SECRET_ACCESS_KEY=verySecretKey1 S3_ENDPOINT_URL=127.0.0.1:8000 go run main.go

This should work both locally and remotely.

*/

func main() {

	nBucket := 500

	// Create an s3 bucket with the name "my-bucket"
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint("http://"+os.Getenv("S3_ENDPOINT_URL")+"/"),
		config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}),
	)
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := s3.NewFromConfig(cfg)
	group, _ := errgroup.WithContext(context.Background())
	group.SetLimit(400)

	bucketNumber := atomic.Int32{}

	// Generate random prefix
	base := rand.Intn(4e9)

	for i := 0; i < nBucket; i++ {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		group.Go(func() error {
			bucketName := fmt.Sprintf("test-%010d-%010d", base, bucketNumber.Add(1))
			hammerWithRequests(client, bucketName)
			return nil

		})
	}

	if err := group.Wait(); err != nil {
		fmt.Println("error: ", err)
	}

	fmt.Println("waiting for ghost buckets to appear")
	time.Sleep(30 * time.Second)
	fmt.Println("checking for ghost buckets")
	ghostBuckets := make([]bool, nBucket)

	for i := 0; i < nBucket; i++ {
		i := i
		group.Go(func() error {
			bucketName := fmt.Sprintf("test-%010d-%010d", base, i)
			fmt.Println("checking bucket for ghostness: ", bucketName)
			ghost, err := isGhostBucket(client, bucketName)
			if err != nil {
				return err
			}
			ghostBuckets[i] = ghost
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		fmt.Println("error: ", err)
	}

	for i, ghost := range ghostBuckets {
		if ghost {
			fmt.Printf("bucketName: %s  isGhostBucket: %t\n", fmt.Sprintf("test-%010d-%010d", base, i), ghost)
		}
	}

	bucketNumber.Store(0)

	group.SetLimit(100)
	for i := 0; i < nBucket; i++ {
		group.Go(func() error {
			bucketN := bucketNumber.Add(1) - 1
			if !ghostBuckets[bucketN] {
				return nil
			}
			bucketName := fmt.Sprintf("test-%010d-%010d", base, bucketN)
			fmt.Println("attempting to recreate bucket: ", bucketName)
			for range make([]struct{}, 50) {
				client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
					Bucket: &bucketName,
				})
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		})
	}

	group.Wait()

	deepGhostBuckets := make([]bool, nBucket)

	for i := 0; i < nBucket; i++ {
		i := i
		group.Go(func() error {
			bucketName := fmt.Sprintf("test-%010d-%010d", base, i)
			ghost, err := isGhostBucket(client, bucketName)
			if err != nil {
				return err
			}
			deepGhostBuckets[i] = ghost
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		fmt.Println("error: ", err)
	}

	for i, ghost := range deepGhostBuckets {
		if ghost {
			fmt.Printf("bucketName: %s  isDeepGhostBucket: %t\n", fmt.Sprintf("test-%010d-%010d", base, i), ghost)
		}
	}
}

func isGhostBucket(client *s3.Client, bucketName string) (bool, error) {

	var existsInList, existsInHead bool

	out, err := client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		return false, err
	}
	for _, b := range out.Buckets {
		if *b.Name == bucketName {
			existsInList = true
		}
	}
	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: &bucketName,
	})
	existsInHead = err == nil

	return existsInList && !existsInHead, nil
}

func hammerWithRequests(client *s3.Client, bucketName string) error {
	client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	group, _ := errgroup.WithContext(context.Background())
	for i := 0; i < 100; i++ {
		group.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
			ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(1000*time.Millisecond))
			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: &bucketName,
			})
			if err != nil {
				fmt.Println("error creating bucket", bucketName, ": ", err)
			}
			return nil
		})
		group.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(1000*time.Millisecond))
			_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: &bucketName,
			})
			if err != nil {
				fmt.Println("error deleting bucket", bucketName, ": ", err)
			}
			return nil
		})
	}
	group.Wait()
	return nil
}
