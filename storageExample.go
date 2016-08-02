// This package demonstrates the usage of Azure Blob Storage services using Go.
package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/Azure/azure-sdk-for-go/storage"
)

func main() {
	fmt.Println("Azure Storage Blob Sample")
	err := blobSamples("demoblobconatiner", "demoPageBlob", "demoAppendBlob", "demoBlockBlob")
	printError(err)
}

// blobSamples creates a container, and performs operations with page blobs, append blobs and block blobs.
func blobSamples(containerName, pageBlobName, appendBlobName, blockBlobName string) error {
	fmt.Println("Get credentials...")
	credentials := map[string]string{
		"AZURE_STORAGE_ACCOUNT_NAME": os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"),
		"AZURE_STORAGE_ACCOUNT_KEY":  os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")}
	if err := checkEnvVar(&credentials); err != nil {
		return err
	}

	fmt.Println("Create storage client...")
	client, err := storage.NewBasicClient(credentials["AZURE_STORAGE_ACCOUNT_NAME"], credentials["AZURE_STORAGE_ACCOUNT_KEY"])
	if err != nil {
		return err
	}

	fmt.Println("Create blob client...")
	blobClient := client.GetBlobService()

	fmt.Println("Create container with private access type...")
	if _, err := blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypePrivate); err != nil {
		return err
	}

	printError(appendBlobOperations(&blobClient, containerName, appendBlobName))
	printError(blockBlobOperations(&blobClient, containerName, blockBlobName))
	printError(pageBlobOperations(&blobClient, containerName, pageBlobName))

	/*
		fmt.Println("Delete container...")
		if _, err = blobClient.DeleteContainerIfExists(containerName); err != nil {
			return err
		}
	*/
	return nil
}

// appendBlobOperations performs simple append blob operations.
// ExtraHeaders adds metadata to the blob (in this example, Content-Type).
// For more information, please visit: https://msdn.microsoft.com/library/azure/ee691975.aspx
func appendBlobOperations(blobClient *storage.BlobStorageClient, containerName, appendBlobName string) error {
	fmt.Println("Create an empty append blob...")
	extraHeaders := map[string]string{
		"Content-Type": "text/plain"}
	if err := blobClient.PutAppendBlob(containerName, appendBlobName, extraHeaders); err != nil {
		return err
	}

	fmt.Println("Append a block to the blob...")
	appendData := randomData(42) //Append blocks can have any length.
	if err := blobClient.AppendBlock(containerName, appendBlobName, appendData, extraHeaders); err != nil {
		return err
	}

	printError(downloadBlob(blobClient, containerName, appendBlobName, "appendBlob.txt"))
	printError(printBlobList(blobClient, containerName))

	/*
		fmt.Println("Delete append blob...")
		if _, err := blobClient.DeleteBlobIfExists(containerName, appendBlobName, nil); err != nil {
			return err
		}
	*/
	return nil
}

// blockBlobOperations performs simple block blob operations.
// Blocks of the the block blob are uploaded with PutBlock function.
// Once all the blocks are uploaded, PutBlockList is used to write/commit those blocks into the blob.
// For more information, please visit: https://msdn.microsoft.com/library/azure/ee691964.aspx
func blockBlobOperations(blobClient *storage.BlobStorageClient, containerName, blockBlobName string) error {
	fmt.Println("Create an empty block blob...")
	if err := blobClient.CreateBlockBlob(containerName, blockBlobName); err != nil {
		return err
	}

	fmt.Println("Upload a block...")
	blockID := base64.StdEncoding.EncodeToString([]byte("foo"))
	blockData := randomData(1984)
	if err := blobClient.PutBlock(containerName, blockBlobName, blockID, blockData); err != nil {
		return err
	}

	printError(printBlockList(blobClient, containerName, blockBlobName))

	fmt.Println("Build uncommitted blocks list...")
	blocksList, err := blobClient.GetBlockList(containerName, blockBlobName, storage.BlockListTypeUncommitted)
	if err != nil {
		return err
	}
	uncommittedBlocksList := make([]storage.Block, len(blocksList.UncommittedBlocks))
	for i := range blocksList.UncommittedBlocks {
		uncommittedBlocksList[i].ID = blocksList.UncommittedBlocks[i].Name
		uncommittedBlocksList[i].Status = storage.BlockStatusUncommitted
	}
	fmt.Println("Commit blocks...")
	if err = blobClient.PutBlockList(containerName, blockBlobName, uncommittedBlocksList); err != nil {
		return err
	}

	printError(printBlockList(blobClient, containerName, blockBlobName))
	printError(downloadBlob(blobClient, containerName, blockBlobName, "blockBlob.txt"))
	printError(printBlobList(blobClient, containerName))

	/*
		fmt.Println("Delete block blob...")
		if _, err = blobClient.DeleteBlobIfExists(containerName, blockBlobName, nil); err != nil {
			return err
		}
	*/
	return nil
}

// pageBlobOperations performs simple page blob operations.
// ExtraHeaders adds metadata to the blob (in this example, Content-Type).
// For more information, please visit: https://msdn.microsoft.com/library/azure/ee691975.aspx
func pageBlobOperations(blobClient *storage.BlobStorageClient, containerName, pageBlobName string) error {
	fmt.Println("Create an empty page blob...")
	extraHeaders := map[string]string{
		"Content-Type": "text/plain"}
	// Page blobs' sizes must be multiples of 512.
	if err := blobClient.PutPageBlob(containerName, pageBlobName, int64(512*5), extraHeaders); err != nil {
		return err
	}

	fmt.Println("Writing in the page blob...")
	pageLen := 512 * 3
	pageData := randomData(pageLen)
	// PutPage can update or clear data in the page blob, just change the writeType argument.
	// The range for the chunks inside the blob must be aligned to 512 byte boundaries.
	if err := blobClient.PutPage(containerName, pageBlobName, 0, int64(pageLen-1), storage.PageWriteTypeUpdate, pageData, extraHeaders); err != nil {
		return err
	}

	fmt.Println("Get valid page ranges...")
	pageRanges, err := blobClient.GetPageRanges(containerName, pageBlobName)
	printError(err)
	if err == nil {
		fmt.Println("Valid page ranges:")
		for _, pageRange := range pageRanges.PageList {
			fmt.Printf("\tFrom page %v to page %v\n", pageRange.Start, pageRange.End)
		}
	}

	printError(downloadBlob(blobClient, containerName, pageBlobName, "pageBlob.txt"))
	printError(printBlobList(blobClient, containerName))

	/*
		fmt.Println("Delete page blob...")
		if _, err = blobClient.DeleteBlobIfExists(containerName, pageBlobName, nil); err != nil {
			return err
		}
	*/
	return nil
}

// checkEnvVar checks if the environment variables are actually set.
func checkEnvVar(envVars *map[string]string) error {
	var missingVars []string
	for name, v := range *envVars {
		if v == "" {
			missingVars = append(missingVars, name)
		}
	}
	if len(missingVars) > 0 {
		return fmt.Errorf("Missing environment variables %v", missingVars)
	}
	return nil
}

// printError prints non nil errors.
func printError(err error) {
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
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
func downloadBlob(blobClient *storage.BlobStorageClient, containerName, blobName, fileName string) error {
	fmt.Printf("Download blob data into '%v'...\n", fileName)
	if _, err := os.Stat(fileName); err == nil {
		return fmt.Errorf("File '%v' already exists", fileName)
	}
	readCloser, err := blobClient.GetBlob(containerName, blobName)
	if err != nil {
		return err
	}
	bytesRead, err := ioutil.ReadAll(readCloser)
	defer readCloser.Close()
	if err != nil {
		return err
	}
	return ioutil.WriteFile(fileName, bytesRead, 0666)
}

// printBlockList prints both committed and uncommitted blocks on a block blob.
func printBlockList(blobClient *storage.BlobStorageClient, containerName, blockBlobName string) error {
	fmt.Println("Get block list...")
	list, err := blobClient.GetBlockList(containerName, blockBlobName, storage.BlockListTypeAll)
	if err != nil {
		return err
	}
	fmt.Printf("Block blob '%v' block list\n", blockBlobName)
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
// For more information, please visit: https://godoc.org/github.com/Azure/azure-sdk-for-go/storage#ListBlobsParameters
func printBlobList(blobClient *storage.BlobStorageClient, containerName string) error {
	fmt.Printf("Get blob list from container '%v'...\n", containerName)
	list, err := blobClient.ListBlobs(containerName, storage.ListBlobsParameters{})
	if err != nil {
		return err
	}
	fmt.Printf("Blobs inside '%v' container:\n", containerName)
	for _, b := range list.Blobs {
		fmt.Printf("\t%v\n", b.Name)
	}
	return nil
}
