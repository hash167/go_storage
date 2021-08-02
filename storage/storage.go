package storage

import (
    "os"
    "fmt"
    "strings"
    "errors"
    "mime/multipart"
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
}

type UploadFileOptions struct {
    Acl string
    ServerSideEncryption string
    KmsKeyId string
    ContentType string
    DisableMultipart bool
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
    region string) *AmazonS3Backend {
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

func (b AmazonS3Backend) UploadObject(
    remotePath string,
    localFile *multipart.FileHeader,
    options UploadFileOptions) error {
    // fmt.Printf("Local Path is %s", localPath)
    // stat, err := os.Stat(localPath)
    // if err != nil {
    //     fmt.Println("Unable to find file in local path")
    //     return err
    // }
    // localFile, err := os.Open(localPath)
    // if err != nil {
    //     fmt.Println("Unable to open local file")
    //     return err
    // }
    // defer localFile.Close()

    // fSize := int64(len(localFile))
    uploader := b.Uploader
    // if !options.DisableMultipart {
	// 	if fSize > int64(uploader.MaxUploadParts)*uploader.PartSize {
	// 		partSize := fSize / int64(uploader.MaxUploadParts)
	// 		if fSize%int64(uploader.MaxUploadParts) != 0 {
	// 			partSize++
	// 		}
	// 		uploader.PartSize = partSize
	// 	}
	// } else {
	// 	uploader.MaxUploadParts = 1
	// 	uploader.Concurrency = 1
	// 	uploader.PartSize = fSize + 1
	// 	if fSize <= s3manager.MinUploadPartSize {
	// 		uploader.PartSize = s3manager.MinUploadPartSize
	// 	}
	// }
    localFileIO, _ := localFile.Open()
    uploadInput := s3manager.UploadInput{
        Bucket: aws.String(b.Bucket),
        Key: aws.String(remotePath),
        Body: localFileIO,
        ACL: aws.String(options.Acl),
    }
    if options.ServerSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(options.ServerSideEncryption)
	}
	if options.KmsKeyId != "" {
		uploadInput.SSEKMSKeyId = aws.String(options.KmsKeyId)
	}
	if options.ContentType != "" {
		uploadInput.ContentType = aws.String(options.ContentType)
	}
    _, err := uploader.Upload(&uploadInput)
	if err != nil {
        fmt.Println("Unable to Upload file")
		return err
	}
    return nil
}

