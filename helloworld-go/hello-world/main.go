package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	// DefaultHTTPGetAddress Default Address
	DefaultHTTPGetAddress = "https://checkip.amazonaws.com"

	// ErrNoIP No IP found in response
	ErrNoIP = errors.New("No IP in HTTP response")

	// ErrNon200Response non 200 status code in response
	ErrNon200Response = errors.New("Non 200 Response found")
)

type App struct {
	S3 *s3.S3
}

type CsvEntry struct {
	ColA string
	ColB string
	ColC string
	ColD string
	ColE string
	ColF string
	ColG string
	ColH string
	ColI string
	ColJ string
}

type FileParseResult struct {
	FileName string
	RowCount int
}

func parseCsvEntry(data [][]string) []CsvEntry {
	var entryList []CsvEntry
	for i, line := range data {
		if i > 0 { // omit header line
			var entry CsvEntry
			for j, field := range line {
				if j == 0 {
					entry.ColA = field
				} else if j == 1 {
					entry.ColB = field
				} else if j == 2 {
					entry.ColB = field
				} else if j == 3 {
					entry.ColB = field
				} else if j == 4 {
					entry.ColB = field
				} else if j == 5 {
					entry.ColB = field
				} else if j == 6 {
					entry.ColB = field
				} else if j == 7 {
					entry.ColB = field
				} else if j == 8 {
					entry.ColB = field
				} else if j == 9 {
					entry.ColB = field
				} else if j == 10 {
					entry.ColB = field
				}
			}
			entryList = append(entryList, entry)
		}
	}
	return entryList
}

func (app App) listBucketContent(bucketName string) string {
	req, err := app.S3.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log.Fatalf("Unable list objects in %q, %v", bucketName, err)
	}

	out, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}

	return string(out)
}

func (app App) retrieveFileSize(bucketName, fileName string) string {

	rawData, err := app.S3.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fileName),
		})

	if err != nil {
		log.Fatalf("Unable to get object %q, %v", fileName, err)
	}
	return fmt.Sprint("Downloaded", rawData.Metadata, rawData.ContentLength, "bytes")
}

func (app App) parseAndCount(bucketName string) []FileParseResult {
	req, err := app.S3.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log.Fatalf("Unable list objects in %q, %v", bucketName, err)
	}

	var fileParseResults []FileParseResult
	for _, file := range req.Contents {
		var entryList []CsvEntry

		rawData, err := app.S3.GetObject(
			&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(*file.Key),
			})

		if err != nil {
			log.Fatalf("Unable to get object %q, %v", *file.Key, err)
		}

		csvReader := csv.NewReader(rawData.Body)
		data, err := csvReader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		entryList = parseCsvEntry(data)
		fileParseResults = append(fileParseResults, FileParseResult{FileName: *file.Key, RowCount: len(entryList)})
	}
	return fileParseResults
}

func (app App) handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	resp, err := http.Get(DefaultHTTPGetAddress)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	if resp.StatusCode != 200 {
		return events.APIGatewayProxyResponse{}, ErrNon200Response
	}

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	if len(ip) == 0 {
		return events.APIGatewayProxyResponse{}, ErrNoIP
	}

	parseResults := app.parseAndCount("testperf-bucket")
	out, err := json.Marshal(parseResults)
	if err != nil {
		panic(err)
	}

	return events.APIGatewayProxyResponse{
		Body:       string(out),
		StatusCode: 200,
	}, nil
}

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	})

	if err != nil {
		log.Fatalf("failed to create AWS session, %v", err)
	}

	s3 := s3.New(sess)
	app := App{S3: s3}
	lambda.Start(app.handler)
}
