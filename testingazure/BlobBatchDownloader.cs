// testingazure/BlobBatchDownloader.cs  (BEFORE - intentionally inefficient)
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace TestingAzure
{
    public class BlobBatchDownloader
    {
        private readonly BlobContainerClient _container;

        public BlobBatchDownloader(string connectionString, string containerName)
        {
            _container = new BlobContainerClient(connectionString, containerName);
        }

        // Inefficient pattern:
        //  - Checks ExistsAsync for each blob (extra transaction)
        //  - Calls GetPropertiesAsync before download (another extra call)
        //  - Downloads sequentially (no parallelization)
        public async Task DownloadAllSequentialAsync(IEnumerable<string> blobNames, string localFolder)
        {
            Directory.CreateDirectory(localFolder);

            foreach (var name in blobNames)
            {
                var blob = _container.GetBlobClient(name);

                // ❌ Redundant existence check (extra transaction)
                if (!(await blob.ExistsAsync()))
                {
                    Console.WriteLine($"Skip missing blob: {name}");
                    continue;
                }

                // ❌ Redundant properties read (another transaction)
                var props = await blob.GetPropertiesAsync();
                Console.WriteLine($"Downloading {name} ({props.Value.ContentLength} bytes)");

                var localPath = Path.Combine(localFolder, name.Replace("/", "_"));
                using (var fs = File.Open(localPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    // Sequential, per-blob call
                    await blob.DownloadToAsync(fs);
                }
            }
        }
    }
}
