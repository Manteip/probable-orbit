using Azure.Storage.Blobs;
using System.Threading.Tasks;

namespace TestingAzure
{
    public class InlineCostTester
    {
        private readonly BlobClient _blob;

        public InlineCostTester(string conn, string container, string blobName)
        {
            _blob = new BlobContainerClient(conn, container).GetBlobClient(blobName);
        }

        // Bad pattern: Existence pre-check
        public async Task<bool> BlobExistsThenDownloadAsync()
        {
            if (await _blob.ExistsAsync())
            {
                await _blob.DownloadAsync();
                return true;
            }
            return false;
        }
    }
}
