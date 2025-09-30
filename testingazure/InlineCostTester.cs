using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace TestingAzure
{
    public class InlineCostTester
    {
        private readonly BlobClient _blob;

        public InlineCostTester(BlobClient blob) { _blob = blob; }

        public async Task<bool> BlobExistsAsync()
        {
            // Intentional cost smell so the bot leaves an inline comment:
            return (await _blob.ExistsAsync()).Value;
        }
    }
}
