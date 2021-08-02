package main

import (
	"os"
	"net/http"
	"mime/multipart"
	"github.com/aws/aws-sdk-go/service/s3"
  	"github.com/gin-gonic/gin"
	"fmt"
	"path/filepath"
	"go_storage/storage"
)
const Bucket = "golang-object-upload"
const Region = "us-west-2"
const DownloadsDir = "downloads"


func get_keys(key_map map [string]*s3.Object) []string {
	keys := []string {}
	for k := range key_map {
		keys = append(keys, k)
	}
	return keys
}

func makeDirectoryIfNotExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.Mkdir(path, os.ModeDir|0755)
	}
	return nil
}

func main(){
	// Make downloads directory to serve for API
	makeDirectoryIfNotExists(DownloadsDir)
	router := gin.Default()
	s3Bucket := storage.NewAmazonS3Backend(
        Bucket,
        Region,
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
		c.JSON(200, gin.H{"data": keys})
	})
	router.GET("/bucket/:prefix/download/", func(c *gin.Context) {
		prefix := c.Param("prefix")
		path := filepath.Join("./downloads", prefix)
		err := s3Bucket.GetObject(prefix, path)
		if err != nil {
			fmt.Printf("Error Downloading file. Error is %v", err)
		}
		image_path := "/image/" + prefix
		c.JSON(200, gin.H{"download": "ok", "path": image_path})
	})

	type UploadScriptInput struct {
		Prefix string	`form:"prefix" binding:"required"`
		LocalFile *multipart.FileHeader `form:"local" binding:"required"`
	}

	router.POST("/bucket", func(c *gin.Context) {
		var input UploadScriptInput
		if err := c.Bind(&input); err != nil {
			fmt.Printf("Some error")
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    		return
		}
		options := storage.UploadFileOptions {
			Acl: "private",
			ServerSideEncryption: "",
			KmsKeyId: "",
			ContentType: "",
			DisableMultipart: false,
		}
		s3Bucket.UploadObject(
			input.Prefix + "/" + input.LocalFile.Filename,
			input.LocalFile,
			options,
		)
		c.JSON(http.StatusOK, gin.H{"upload": "completed"})

	})
	router.Static("/image", "./downloads")

	router.Run(":8080")
    
}