using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace TestingAzure
{
    public class InlineCostTester
    {
        private readonly BlobClient _blob;

        public InlineCostTester(BlobClient blob)
        {
            _blob = blob;
        }

        public async Task<bool> BlobExistsAsync()
        {
            // Inefficient existence check – expect inline bot comment here
            return await _blob.ExistsAsync();
        }
    }
}
