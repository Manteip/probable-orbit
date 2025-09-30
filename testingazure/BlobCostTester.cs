using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

public class BlobCostTester
{
    private readonly BlobServiceClient _client;

    public BlobCostTester(string connectionString)
    {
        _client = new BlobServiceClient(connectionString);
    }

    // ✅ Cost-efficient: directly attempts to download, handles exception if missing
    public async Task<byte[]> GetBlobBytesAsync(string container, string blobName)
    {
        try
        {
            var containerClient = _client.GetBlobContainerClient(container);
            var blobClient = containerClient.GetBlobClient(blobName);

            var response = await blobClient.DownloadContentAsync();
            return response.Value.Content.ToArray();
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return Array.Empty<byte[]>();
        }
    }
}
