package main

import (
	"github.com/aws/aws-sdk-go/service/s3"
  	"github.com/gin-gonic/gin"
	"fmt"
	"go_storage/storage"
)
const Bucket = "media-query-mediabucket-1lauc6ptnwz0g"
const Region = "us-west-2"


func get_keys(key_map map [string]*s3.Object) []string {
	keys := []string {}
	for k := range key_map {
		keys = append(keys, k)
	}
	return keys
}

func main(){
	router := gin.Default()
	s3Bucket := storage.NewAmazonS3Backend(
        Bucket,
        Region,
        "sse",
    )
  
	router.GET("/bucket", func(c *gin.Context){
		objs, err := s3Bucket.ListObjects("/")
		if err != nil {
			fmt.Printf("Error is %v", err)
		}
		keys := get_keys(objs)
		c.JSON(200, gin.H{"keys": keys})
	})
	
	router.GET("/bucket/:prefix", func(c *gin.Context) {
		prefix := c.Param("prefix")
		objs, err := s3Bucket.ListObjects(prefix)
		if err != nil {
			fmt.Printf("Error is %v", err)
		}
		keys := get_keys(objs)
		c.JSON(200, gin.H{"keys": keys})
	})

	router.Run(":8080")
    
    // fmt.Printf("Bucket Name is %s\n", s3Bucket.Bucket)
    
    // fmt.Printf("Bucket files are:\n")
    // for k := range objs{
    //     fmt.Printf("->%s\n", k)
    // }
    // err = s3Bucket.GetObject("/sample.jpg", "/Users/random_number/Downloads/sample.jpg")
    // if err != nil {
    //     fmt.Printf("Error Downloading file. Error is %v", err)
    // }
}