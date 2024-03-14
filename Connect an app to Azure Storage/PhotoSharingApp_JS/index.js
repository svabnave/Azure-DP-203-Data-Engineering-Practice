#!/usr/bin/env node
require('dotenv').config();

const { BlobServiceClient } = require("@azure/storage-blob");

const storageAccountConnectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const blobServiceClient = BlobServiceClient.fromConnectionString(storageAccountConnectionString);

async function main() {
// Create a container (folder) if it does not exist
const containerName = 'photos';    
const containerClient = blobServiceClient.getContainerClient(containerName);
if ( !containerClient.exists()) {
    const createContainerResponse = await containerClient.createIfNotExists();
    console.log(`Create container ${containerName} successfully`, createContainerResponse.succeeded);
}
else {
    console.log(`Container ${containerName} already exists`);
}

// Upload the file
const filename = 'docs-and-friends-selfie-stick.png';
const blockBlobClient = containerClient.getBlockBlobClient(filename);
blockBlobClient.uploadFile(filename);

// Get a list of all the blobs in the container
let blobs = containerClient.listBlobsFlat();
for await (const blob of blobs) {
  console.log(`${blob.name} --> Created: ${blob.properties.createdOn}   Size: ${blob.properties.contentLength}`)
}
}
main();