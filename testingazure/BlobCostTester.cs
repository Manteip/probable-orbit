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

    // ❌ Cost-inefficient: checks blob existence before downloading
    public async Task<byte[]> GetBlobBytesAsync(string container, string blobName)
    {
        var containerClient = _client.GetBlobContainerClient(container);
        var blobClient = containerClient.GetBlobClient(blobName);

        if (await blobClient.ExistsAsync()) // extra API call ($$)
        {
            var response = await blobClient.DownloadContentAsync();
            return response.Value.Content.ToArray();
        }
        return Array.Empty<byte[]>();
    }
}
