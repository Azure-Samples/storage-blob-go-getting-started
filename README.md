#Getting Started with Azure Blob Service in Go

This example demonstrates how to use the Blob Storage service with Go. If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](https://azure.microsoft.com/pricing/free-trial).

##Instructions

###Using Azure Storage Emulator
1. Download, install and run the [Azure Storage Emulator](https://azure.microsoft.com/documentation/articles/storage-use-emulator/).
2. Get the [Azure SDK for Go](https://github.com/Azure/azure-sdk-for-go) using command `go get -u github.com/Azure/azure-sdk-for-go`
3. Get this sample using command `go get -u github.com/Azure-Samples/storage-blob-go-getting-started`
4. Compile and run the sample.

###Using Storage Service
1. Create a [storage account](https://azure.microsoft.com/documentation/articles/storage-create-storage-account/#create-a-storage-account) through the Azure Portal.
2. Set environment variables `AZURE_STORAGE_ACCOUNT_NAME = <ACCOUNT_NAME>` and `AZURE_STORAGE_ACCOUNT_KEY = <ACCOUNT_KEY>`.
3. Get the [Azure SDK for Go](https://github.com/Azure/azure-sdk-for-go) using command `go get -u github.com/Azure/azure-sdk-for-go`
4. Get this sample using command `go get -u github.com/Azure-Samples/storage-blob-go-getting-started`
5. Open the storageExample.go file and replace this line `credentials, err := getCredentials(emulator)` with this line `credentials, err := getCredentials(account)`, inside the blobSamples function.
6. Compile and run the sample.

##Find documentation
- [About Azure storage accounts](https://azure.microsoft.com/documentation/articles/storage-create-storage-account/)
- [Get started with Azure Blob - Blob service concepts](https://azure.microsoft.com/documentation/articles/storage-dotnet-how-to-use-blobs/#blob-service-concepts) - This link is for .NET, but the blob service concepts are the same
- [Blob Service Concepts](https://msdn.microsoft.com/library/dd179376.aspx)
- [Blob Service REST API](https://msdn.microsoft.com/library/dd135733.aspx)
- [Azure Storage Emulator](https://azure.microsoft.com/documentation/articles/storage-use-emulator/)

***

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.