package s3_log

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func generateRandomStr() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func setupMinioClient() *s3.Client {
	// https://stackoverflow.com/a/78815403
	// thank you lurenyang
	return s3.NewFromConfig(aws.Config{Region: "us-east-1"}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
		o.Credentials = credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	})
}

func setupBucket(client *s3.Client, bucketName string) error {
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	// if the bucket already exists, ignore the error
	var bae *types.BucketAlreadyExists
	var boe *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &bae) && !errors.As(err, &boe) {
		return err
	}
	return nil
}

// emptyBucket deletes the bucket because dumbass AWS does not have a direct API
func emptyBucket(ctx context.Context, client *s3.Client, bucketName, prefix string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}
		if len(output.Contents) == 0 {
			continue
		}
		objectIds := make([]types.ObjectIdentifier, len(output.Contents))
		for i, object := range output.Contents {
			objectIds[i] = types.ObjectIdentifier{
				Key: object.Key,
			}
		}
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectIds,
				Quiet:   aws.Bool(false),
			},
		}
		_, err = client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}

	return nil
}

func getWAL(t *testing.T) (*S3WAL, func()) {
	client := setupMinioClient()
	bucketName := "test-wal-bucket-" + generateRandomStr()
	prefix := generateRandomStr()

	if err := setupBucket(client, bucketName); err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := emptyBucket(context.Background(), client, bucketName, prefix); err != nil {
			t.Logf("failed to empty bucket during cleanup: %v", err)
		}
		_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Logf("failed to delete bucket during cleanup: %v", err)
		}
	}
	return NewS3WAL(client, bucketName, prefix), cleanup
}

func TestAppendAndReadSingle(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()
	testData := []byte("hello world")

	offset, err := wal.Append(ctx, testData)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if offset != 1 {
		t.Errorf("expected first offset to be 1, got %d", offset)
	}

	record, err := wal.Read(ctx, offset)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if record.Offset != offset {
		t.Errorf("offset mismatch: expected %d, got %d", offset, record.Offset)
	}

	if string(record.Data) != string(testData) {
		t.Errorf("data mismatch: expected %q, got %q", testData, record.Data)
	}
}
func TestAppendMultiple(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()

	testData := [][]byte{
		[]byte("Do not answer. Do not answer. Do not answer."),
		[]byte("I am a pacifist in this world. You are lucky that I am first to receive your message."),
		[]byte("I am warning you: do not answer. If you respond, we will come. Your world will be conquered"),
		[]byte("Do not answer."),
	}

	var offsets []uint64
	for _, data := range testData {
		offset, err := wal.Append(ctx, data)
		if err != nil {
			t.Fatalf("failed to append: %v", err)
		}
		offsets = append(offsets, offset)
	}

	for i, offset := range offsets {
		record, err := wal.Read(ctx, offset)
		if err != nil {
			t.Fatalf("failed to read offset %d: %v", offset, err)
		}

		if record.Offset != offset {
			t.Errorf("offset mismatch: expected %d, got %d", offset, record.Offset)
		}

		if string(record.Data) != string(testData[i]) {
			t.Errorf("data mismatch at offset %d: expected %q, got %q",
				offset, testData[i], record.Data)
		}
	}
}

func TestAppendMultipleConcurrency(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()

	// Random data generation
	numData := 100
	data := make([][]byte, numData)
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	for i := range data {
		dataLen := rand.Intn(100) + 1 // Generate random data length between 1 and 100
		data[i] = make([]byte, dataLen)
		for j := range data[i] {
			data[i][j] = byte(rand.Intn(256)) // Generate random byte values
		}
	}

	var wg sync.WaitGroup
	offsets := make([]uint64, len(data)) // Pre-allocate the offsets slice

	for i, data := range data {
		wg.Add(1)
		go func(i int, data []byte) {
			defer wg.Done()

			offset, err := wal.Append(ctx, data)
			if err != nil {
				t.Errorf("failed to append data %d: %v", i, err)
				return // Exit the goroutine on error
			}
			offsets[i] = offset
		}(i, data)
	}

	// Wait for all goroutines to finish appending
	wg.Wait()

	// Now read and verify data
	for i, offset := range offsets {
		record, err := wal.Read(ctx, offset)
		if err != nil {
			t.Fatalf("failed to read offset %d: %v", offset, err)
		}

		if record.Offset != offset {
			t.Errorf("offset mismatch: expected %d, got %d", offset, record.Offset)
		}

		if string(record.Data) != string(data[i]) {
			t.Errorf("data mismatch at offset %d: expected %q, got %q",
				offset, data[i], record.Data)
		}
	}
}

func TestReadNonExistent(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	_, err := wal.Read(context.Background(), 99999)
	if err == nil {
		t.Error("expected error when reading non-existent record, got nil")
	}
}

func TestAppendEmpty(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()

	offset, err := wal.Append(ctx, []byte{})
	if err != nil {
		t.Fatalf("failed to append empty data: %v", err)
	}

	record, err := wal.Read(ctx, offset)
	if err != nil {
		t.Fatalf("failed to read empty record: %v", err)
	}

	if len(record.Data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(record.Data))
	}
}

func TestAppendLarge(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()

	largeData := make([]byte, 10*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	offset, err := wal.Append(ctx, largeData)
	if err != nil {
		t.Fatalf("failed to append large data: %v", err)
	}

	record, err := wal.Read(ctx, offset)
	if err != nil {
		t.Fatalf("failed to read large record: %v", err)
	}

	if len(record.Data) != len(largeData) {
		t.Errorf("data length mismatch: expected %d, got %d",
			len(largeData), len(record.Data))
	}

	for i := range largeData {
		if record.Data[i] != largeData[i] {
			t.Errorf("data mismatch at index %d: expected %d, got %d",
				i, largeData[i], record.Data[i])
			break
		}
	}
}

func TestSameOffset(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()
	// https://x.com/iavins/status/1860299083056849098
	data := []byte("threads are evil")
	_, err := wal.Append(ctx, data)
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	// reset the WAL counter so that it uses the same offset
	wal.length = 0
	_, err = wal.Append(ctx, data)
	if err == nil {
		t.Error("expected error when appending at same offset, got nil")
	}
}

func TestLastRecord(t *testing.T) {
	wal, cleanup := getWAL(t)
	defer cleanup()
	ctx := context.Background()

	record, err := wal.LastRecord(ctx)
	if err == nil {
		t.Error("expected error when getting last record from empty WAL, got nil")
	}

	var lastData []byte
	for i := 0; i < 1234; i++ {
		lastData = []byte(generateRandomStr())
		_, err = wal.Append(ctx, lastData)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	record, err = wal.LastRecord(ctx)
	if err != nil {
		t.Fatalf("failed to get last record: %v", err)
	}

	if record.Offset != 1234 {
		t.Errorf("expected offset 1234, got %d", record.Offset)
	}

	if string(record.Data) != string(lastData) {
		t.Errorf("data mismatch: expected %q, got %q", lastData, record.Data)
	}
}
