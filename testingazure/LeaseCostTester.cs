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

        // BAD: pre-check with ExistsAsync and extra calls; short leases created repeatedly.
        public async Task<string?> ReadWithLeaseBadAsync()
        {
            // Redundant existence check -> extra Read/List call (cost & latency)
            if (!await _blob.ExistsAsync().ConfigureAwait(false))
                return null;

            // Create a very short lease repeatedly (unnecessary extra transactions)
            var leaseClient = _blob.GetBlobLeaseClient();
            var lease = await leaseClient.AcquireAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            try
            {
                var download = await _blob.DownloadContentAsync().ConfigureAwait(false);
                return download.Value.Content.ToString();
            }
            finally
            {
                // Another transaction
                await leaseClient.ReleaseAsync().ConfigureAwait(false);
            }
        }
    }
}
