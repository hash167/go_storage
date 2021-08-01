package main

import (
	"fmt"
	"go_storage/storage"
)

func main(){
    s3Bucket := storage.NewAmazonS3Backend(
        "media-query-mediabucket-1lauc6ptnwz0g",
        "us-west-2",
        "false",
    )
    fmt.Printf("Bucket Name is %s\n", s3Bucket.Bucket)
    objs, err := s3Bucket.ListObjects("/")
    if err != nil {
        fmt.Printf("Error is %v", err)
    }
    fmt.Printf("Bucket files are:\n")
    for k := range objs{
        fmt.Printf("->%s\n", k)
    }
    err = s3Bucket.GetObject("/sample.jpg", "/Users/random_number/Downloads/sample.jpg")
    if err != nil {
        fmt.Printf("Error Downloading file. Error is %v", err)
    }
}