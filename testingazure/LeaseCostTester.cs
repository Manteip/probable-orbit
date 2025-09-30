using System;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;

namespace TestingAzure
{
    public class LeaseCostTester
    {
        private readonly BlobClient _blob;

        public LeaseCostTester(string connectionString, string container, string name)
        {
            _blob = new BlobContainerClient(connectionString, container).GetBlobClient(name);
        }

        // GOOD: avoid pre-check; attempt read and handle 404. Skip unnecessary lease churn.
        public async Task<string?> ReadWithLeaseGoodAsync()
        {
            try
            {
                // One operation path — fewer storage transactions
                var download = await _blob.DownloadContentAsync().ConfigureAwait(false);
                return download.Value.Content.ToString();
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return null; // blob not found
            }
        }
    }
}
