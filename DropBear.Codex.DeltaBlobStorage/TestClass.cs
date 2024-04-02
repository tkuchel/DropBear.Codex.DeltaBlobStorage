using System.Text;
using FastRsync.Core;
using FastRsync.Delta;
using FastRsync.Diagnostics;
using FastRsync.Signature;
using FluentStorage;
using FluentStorage.Blobs;

namespace DropBear.Codex.DeltaBlobStorage;

public class TestClass
{
    public async Task AzureBlobBasics()
    {
        //create the storage using a factory method
        IBlobStorage storage = StorageFactory.Blobs.AzureBlobStorageWithSharedKey(
            "storage name",
            "storage key");
        

        //upload it
        string content = "test content";
        using (var s = new MemoryStream(Encoding.UTF8.GetBytes(content)))
        {
            await storage.WriteAsync("mycontainer/someid", s);
        }

        //read back
        using (var s = new MemoryStream())
        {
            using (Stream ss = await storage.OpenReadAsync("mycontainer/someid"))
            {
                await ss.CopyToAsync(s);

                //content is now "test content"
                content = Encoding.UTF8.GetString(s.ToArray());
            }
        }
    }

    public async Task FastRsyncBasics()
    {
        // Calculate the signature of the basis file 
        var basisFilePath = "basis.txt";
        var signatureFilePath = "signature.txt";
        var signatureBuilder = new SignatureBuilder();
        using (var basisStream = new FileStream(basisFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var signatureStream = new FileStream(signatureFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            signatureBuilder.Build(basisStream, new SignatureWriter(signatureStream));
        }
        
        
        // Calculate the delta between the basis file and the new file
        var newFilePath = "new.txt";
        var deltaFilePath = "delta.txt";
        var delta = new DeltaBuilder();
        delta.ProgressReport = new ConsoleProgressReporter();
        using (var newFileStream = new FileStream(newFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var signatureStream = new FileStream(signatureFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var deltaStream = new FileStream(deltaFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            delta.BuildDelta(newFileStream, new SignatureReader(signatureStream, delta.ProgressReporter), new AggregateCopyOperationsDecorator(new BinaryDeltaWriter(deltaStream)));
        }
        
        // Apply the delta to the basis file to reconstruct the new file (Patch the file)
        var deltaApplier = new DeltaApplier
        {
            SkipHashCheck = true
        };
        using (var basisStream = new FileStream(basisFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var deltaStream = new FileStream(deltaFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var newFileStream = new FileStream(newFilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read))
        {
            deltaApplier.Apply(basisStream, new BinaryDeltaReader(deltaStream, progressReporter), newFileStream);
        }
        
        // When needing to compress.
        FastRsync.Compression.GZip.Compress(Stream sourceStream, Stream destStream);
        
        // Calculate signature of azure blob 
        
        var storageAccount = CloudStorageAccount.Parse("azure storage connectionstring");
        var blobClient = storageAccount.CreateCloudBlobClient();
        var blobsContainer = blobClient.GetContainerReference("containerName");
        var basisBlob = blobsContainer.GetBlockBlobReference("blobName");

        var signatureBlob = container.GetBlockBlobReference("blob_signature");

        var signatureBuilder2 = new SignatureBuilder();
        using (var signatureStream = await signatureBlob.OpenWriteAsync())
        using (var basisStream = await basisBlob.OpenReadAsync())
        {
            await signatureBuilder2.BuildAsync(basisStream, new SignatureWriter(signatureStream));
        }
    }
}
