package storage

import (
    "os"
    "fmt"
    "strings"
    "errors"
	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func cleanPrefix(prefix string) string {
    return strings.Trim(prefix, "/")
}

type AmazonS3Backend struct {
    Bucket string
    Client *s3.S3
    Downloader *s3manager.Downloader
    Uploader *s3manager.Uploader
    SSE string
}

func (b AmazonS3Backend) ListObjects(prefix string) (map[string]*s3.Object, error) {
    cleaned_prefix := cleanPrefix(prefix)
    bucketContents := map[string]*s3.Object{}
    marker := ""
    for {
        listObjectResponse, err := b.Client.ListObjects(&s3.ListObjectsInput{
            Bucket: aws.String(b.Bucket),
            Prefix: aws.String(cleaned_prefix),
            Marker: aws.String(marker)})
        if err != nil {
            return bucketContents, err
        }
        lastKey := ""
        for _, key := range listObjectResponse.Contents {
            bucketContents[*key.Key] = key
            lastKey = *key.Key
        }

        if *listObjectResponse.IsTruncated {
            prevMarker := marker
            if listObjectResponse.NextMarker == nil {
                marker = lastKey
            } else {
                marker = *listObjectResponse.NextMarker
            }
            if marker == prevMarker {
				return nil, errors.New("Unable to list all bucket objects; perhaps this is a CloudFront S3 bucket that needs its `Query String Forwarding and Caching` set to `Forward all, cache based on all`?")
			}
        } else{
            break
        }
    }

    return bucketContents, nil
}

const maxRetries = 12

func NewAmazonS3Backend(
    bucket string,
    region string, 
    sse string) *AmazonS3Backend {
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
        MaxRetries: aws.Int(maxRetries)},)
    if err != nil {
        fmt.Println("Following error occured", err)
    }
    svc := s3.New(sess)

    backend := &AmazonS3Backend{
        Bucket: bucket,
        Client: svc,
        Downloader: s3manager.NewDownloaderWithClient(svc),
        Uploader: s3manager.NewUploaderWithClient(svc),
        SSE: sse,
    }
    return backend
}

func (b AmazonS3Backend) GetObject(prefix string, localPath string) error {
    svc := b.Client
    input := &s3.HeadObjectInput{
        Bucket: aws.String(b.Bucket),
        Key: aws.String(prefix),
    }
    _, err_head := svc.HeadObject(input)
    if err_head != nil {
        return err_head
    }
    localFile, err_file := os.Create(localPath)
    if err_file != nil {
        return err_file
    }
    defer localFile.Close()
    getObjectInput := &s3.GetObjectInput{
        Bucket: aws.String(b.Bucket),
        Key: aws.String(prefix),
    }
    _, err_d := b.Downloader.Download(localFile, getObjectInput)
    if err_d != nil {
		return err_d
	}

    return nil
}

