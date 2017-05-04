// This package demonstrates the usage of Azure Blob Storage services using Go.
package main

import (
	"bytes"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/Azure/azure-sdk-for-go/storage"
)

var (
	accountName string
	accountKey  string
	blobCli     storage.BlobStorageClient
	emulator    *bool

	appendBlobFile = "appendBlob.txt"
	blockBlobFile  = "blockBlob.txt"
	pageBlobFile   = "pageBlob.txt"
)

func init() {
	emulator = flag.Bool("emulator", false, "use the Azure Storage Emulator")
	flag.Parse()
	if *emulator {
		accountName = storage.StorageEmulatorAccountName
		accountKey = storage.StorageEmulatorAccountKey
	} else {
		accountName = getEnvVarOrExit("ACCOUNT_NAME")
		accountKey = getEnvVarOrExit("ACCOUNT_KEY")
	}
	client, err := storage.NewBasicClient(accountName, accountKey)
	onErrorFail(err, "Create client failed")

	blobCli = client.GetBlobService()
}

func main() {
	fmt.Println("Azure Storage Blob Sample")
	blobSamples("demoblobconatiner", "demoPageBlob", "demoAppendBlob", "demoBlockBlob")
}

// blobSamples creates a container, and performs operations with page blobs, append blobs and block blobs.
func blobSamples(containerName, pageBlobName, appendBlobName, blockBlobName string) {
	fmt.Println("Create container with private access type...")
	cnt := blobCli.GetContainerReference(containerName)
	options := storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	}
	_, err := cnt.CreateIfNotExists(&options)
	if err != nil {
		if accountName == storage.StorageEmulatorAccountName {
			onErrorFail(err, "Create container failed: If you are running with the emulator credentials, plaase make sure you have started the storage emmulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample")
		}
		onErrorFail(err, "Create container failed")
	}

	// Append blobs are not supported in the Azure Storage emulator
	if !*emulator {
		err = appendBlobOperations(cnt, appendBlobName)
		onErrorFail(err, "Append blob operations failed")
	}

	err = blockBlobOperations(cnt, blockBlobName)
	onErrorFail(err, "Block blob operations failed")

	err = pageBlobOperations(cnt, pageBlobName)
	onErrorFail(err, "Page blob operations failed")

	err = printBlobList(cnt)
	onErrorFail(err, "List blobs failed")

	fmt.Print("Press enter to delete the blobs, container and local files created in this sample...")

	var input string
	fmt.Scanln(&input)

	fmt.Println("Delete container...")
	err = cnt.Delete(nil)
	onErrorFail(err, "Delete container failed")

	fmt.Println("Delete files...")
	os.Remove(appendBlobFile)
	os.Remove(blockBlobFile)
	os.Remove(pageBlobFile)

	fmt.Println("Done")
}

// appendBlobOperations performs simple append blob operations.
// For more information, please visit: https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-append-blobs
func appendBlobOperations(cnt storage.Container, appendBlobName string) error {
	fmt.Println("Create an empty append blob...")
	b := cnt.GetBlobReference(appendBlobName)
	b.Properties.ContentType = "text/plain"
	err := b.PutAppendBlob(nil)
	if err != nil {
		return fmt.Errorf("put append blob failed: %v", err)
	}

	fmt.Println("Append a block to the blob...")
	data := randomData(42) //Append blocks can have any length.
	err = b.AppendBlock(data, nil)
	if err != nil {
		return fmt.Errorf("append block failed: %v", err)
	}

	err = downloadBlob(b, appendBlobFile)
	if err != nil {
		return fmt.Errorf("download blob failed: %v", err)
	}

	return nil
}

// blockBlobOperations performs simple block blob operations.
// Blocks of the the block blob are uploaded with PutBlock function.
// Once all the blocks are uploaded, PutBlockList is used to write/commit those blocks into the blob.
// For more information, please visit: https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-block-blobs
func blockBlobOperations(cnt storage.Container, blockBlobName string) error {
	fmt.Println("Create an empty block blob...")
	b := cnt.GetBlobReference(blockBlobName)
	err := b.CreateBlockBlob(nil)
	if err != nil {
		return fmt.Errorf("create block blob failed: %v", err)
	}

	fmt.Println("Put a block...")
	blockID := base64.StdEncoding.EncodeToString([]byte("00000"))
	data := randomData(1984)
	err = b.PutBlock(blockID, data, nil)
	if err != nil {
		return fmt.Errorf("put block failed: %v", err)
	}

	err = printBlockList(b)
	if err != nil {
		return fmt.Errorf("get block list failed: %v", err)
	}

	fmt.Println("Get uncommitted blocks list...")
	list, err := b.GetBlockList(storage.BlockListTypeUncommitted, nil)
	if err != nil {
		return fmt.Errorf("get block list failed: %v", err)
	}
	uncommittedBlocksList := make([]storage.Block, len(list.UncommittedBlocks))
	for i := range list.UncommittedBlocks {
		uncommittedBlocksList[i].ID = list.UncommittedBlocks[i].Name
		uncommittedBlocksList[i].Status = storage.BlockStatusUncommitted
	}

	fmt.Println("Commit blocks...")
	err = b.PutBlockList(uncommittedBlocksList, nil)
	if err != nil {
		return fmt.Errorf("put block list failed: %v", err)
	}

	err = printBlockList(b)
	if err != nil {
		return fmt.Errorf("get block list failed: %v", err)
	}

	err = downloadBlob(b, blockBlobFile)
	if err != nil {
		return fmt.Errorf("download blob failed: %v", err)
	}

	return nil
}

// pageBlobOperations performs simple page blob operations.
// For more information, please visit: https://docs.microsoft.com/en-us/rest/api/storageservices/operations-on-page-blobs
func pageBlobOperations(cnt storage.Container, pageBlobName string) error {
	fmt.Println("Create an empty page blob...")
	b := cnt.GetBlobReference(pageBlobName)
	b.Properties.ContentType = "text/plain"
	// Page blobs' sizes must be multiples of 512.
	b.Properties.ContentLength = int64(512 * 5)
	err := b.PutPageBlob(nil)
	if err != nil {
		return fmt.Errorf("put page blob failed: %v", err)
	}

	fmt.Println("Writing in the page blob...")
	pageLen := 512 * 3
	data := randomData(pageLen)
	// The range for the chunks inside the blob must be aligned to 512 byte boundaries.
	br := storage.BlobRange{
		End: uint64(pageLen - 1),
	}
	err = b.WriteRange(br, bytes.NewReader(data), nil)
	if err != nil {
		return fmt.Errorf("write range failed: %v", err)
	}

	fmt.Println("Get valid page ranges...")
	pageRanges, err := b.GetPageRanges(nil)
	if err != nil {
		return fmt.Errorf("get page ranges failed: %v", err)
	}
	fmt.Println("Valid page ranges:")
	for _, pageRange := range pageRanges.PageList {
		fmt.Printf("\tFrom page %v to page %v\n", pageRange.Start, pageRange.End)
	}

	err = downloadBlob(b, pageBlobFile)
	if err != nil {
		return fmt.Errorf("download blob failed: %v", err)
	}

	return nil
}

// randomData returns a byte array with random bytes.
func randomData(strLen int) []byte {
	ran := 'z' - '0'
	text := make([]byte, strLen)
	for i := range text {
		char := rand.Int()
		char %= int(ran)
		char += '0'
		text[i] = byte(char)
	}
	return text
}

// downloadBlob writes the blob's data into a local file.
func downloadBlob(b storage.Blob, fileName string) error {
	fmt.Printf("Download blob '%v' into '%v'...\n", b.Name, fileName)

	_, err := os.Stat(fileName)
	if err == nil {
		return fmt.Errorf("file '%v' already exists", fileName)
	}

	readCloser, err := b.Get(nil)
	defer readCloser.Close()
	if err != nil {
		return fmt.Errorf("get blob failed: %v", err)
	}

	bytesRead, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return fmt.Errorf("read body failed: %v", err)
	}

	err = ioutil.WriteFile(fileName, bytesRead, 0666)
	if err != nil {
		return fmt.Errorf("write file failed: %v", err)
	}

	return nil
}

// printBlockList prints both committed and uncommitted blocks on a block blob.
func printBlockList(b storage.Blob) error {
	fmt.Println("Get block list...")
	list, err := b.GetBlockList(storage.BlockListTypeAll, nil)
	if err != nil {
		return err
	}
	fmt.Printf("Block blob '%v' block list\n", b.Name)
	fmt.Println("\tCommitted Blocks' IDs")
	for _, b := range list.CommittedBlocks {
		fmt.Printf("\t\t%v\n", b.Name)
	}
	fmt.Println("\tUncommited Blocks' IDs")
	for _, b := range list.UncommittedBlocks {
		fmt.Printf("\t\t%v\n", b.Name)
	}
	return nil
}

// printBlobList prints all blobs' names in a container.
// ListBlobsParameters can customize a blobs list.
// For more information, please visit: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs
func printBlobList(cnt storage.Container) error {
	fmt.Printf("Get blob list from container '%v'...\n", cnt.Name)
	list, err := cnt.ListBlobs(storage.ListBlobsParameters{})
	if err != nil {
		return err
	}
	fmt.Printf("Blobs inside '%v' container:\n", cnt.Name)
	for _, b := range list.Blobs {
		fmt.Printf("\t%v\n", b.Name)
	}
	return nil
}

// getEnvVarOrExit returns the value of specified environment variable or terminates if it's not defined.
func getEnvVarOrExit(varName string) string {
	value := os.Getenv(varName)
	if value == "" {
		fmt.Printf("Missing environment variable %s\n", varName)
		os.Exit(1)
	}

	return value
}

// onErrorFail prints a failure message and exits the program if err is not nil.
func onErrorFail(err error, message string) {
	if err != nil {
		fmt.Printf("%s: %s\n", message, err)
		os.Exit(1)
	}
}
