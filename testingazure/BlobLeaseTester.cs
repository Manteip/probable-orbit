using System;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace TestingAzure
{
    public class BlobLeaseTester
    {
        private readonly BlobClient _blob;

        public BlobLeaseTester(string conn, string container, string name)
        {
            _blob = new BlobServiceClient(conn).GetBlobContainerClient(container).GetBlobClient(name);
        }

        // Cost anti-pattern: existence pre-check + serial path
        public async Task<string?> TryAcquireLeaseThenReadAsync()
        {
            // Existence pre-check = extra GET/HEAD call
            if (!await _blob.ExistsAsync()) return null;

            var bl = _blob.GetBlobLeaseClient();
            var leaseResp = await bl.AcquireAsync(TimeSpan.FromSeconds(15)); // short lease, frequent renews
            try
            {
                var download = await _blob.DownloadContentAsync();           // extra call after exists
                return download.Value.Content.ToString();
            }
            finally
            {
                await bl.ReleaseAsync();
            }
        }
    }
}
