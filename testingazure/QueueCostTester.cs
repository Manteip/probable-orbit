using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace testingazure
{
    /// <summary>
    /// Cost-conscious blob access:
    ///  - Do NOT call Exists() before reads (avoid the extra transaction).
    ///  - Handle 404 via exception filtering.
    ///  - Optional bounded parallelism for multiple downloads.
    /// </summary>
    public class BlobCostTester
    {
        private readonly BlobContainerClient _container;

        public BlobCostTester(BlobContainerClient container)
        {
            _container = container ?? throw new ArgumentNullException(nameof(container));
        }

        /// <summary>
        /// Downloads a blob's content. Returns null if the blob doesn't exist.
        /// </summary>
        public async Task<byte[]?> DownloadAsync(string blobName, CancellationToken ct = default)
        {
            var client = _container.GetBlobClient(blobName);

            try
            {
                // Single call; no pre-check.
                var resp = await client.DownloadContentAsync(ct).ConfigureAwait(false);
                return resp.Value.Content.ToArray();
            }
            // Treat "not found" as a normal outcome, not a second call.
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                return null;
            }
        }

        /// <summary>
        /// Downloads many blobs with bounded concurrency.
        /// </summary>
        public async Task<IDictionary<string, byte[]>> DownloadManyAsync(
            IEnumerable<string> blobNames,
            int maxConcurrency = 8,
            CancellationToken ct = default)
        {
            if (blobNames is null) throw new ArgumentNullException(nameof(blobNames));
            if (maxConcurrency < 1) maxConcurrency = 1;

            var results = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            var semaphore = new SemaphoreSlim(maxConcurrency);

            var tasks = blobNames.Select(async name =>
            {
                await semaphore.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var data = await DownloadAsync(name, ct).ConfigureAwait(false);
                    if (data is not null)
                    {
                        lock (results)
                        {
                            results[name] = data;
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            });

            await Task.WhenAll(tasks).ConfigureAwait(false);
            return results;
        }

        /// <summary>
        /// Example: enumerate blobs and download the first N with bounded concurrency.
        /// </summary>
        public async Task<IReadOnlyDictionary<string, byte[]>> DownloadFirstNAsync(
            int take = 50,
            int maxConcurrency = 8,
            CancellationToken ct = default)
        {
            var names = new List<string>(take);
            await foreach (BlobItem item in _container.GetBlobsAsync(cancellationToken: ct))
            {
                names.Add(item.Name);
                if (names.Count >= take) break;
            }

            var dict = await DownloadManyAsync(names, maxConcurrency, ct).ConfigureAwait(false);
            return new Dictionary<string, byte[]>(dict);
        }
    }
}
