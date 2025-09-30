using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace TestingAzure
{
    /// <summary>
    /// Intentionally inefficient sample to trigger cost recommendations.
    /// Anti-patterns included:
    ///  - Create/Dispose ServiceBusClient and Sender for every message
    ///  - Check queue existence for every send
    ///  - Send single messages instead of a batch
    /// </summary>
    public class QueueCostTester
    {
        private readonly string _connectionString;
        private readonly string _queueName;

        public QueueCostTester(string connectionString, string queueName)
        {
            _connectionString = connectionString;
            _queueName = queueName;
        }

        public async Task SendIndividuallyAsync(IEnumerable<string> payloads)
        {
            foreach (var p in payloads)
            {
                // ❌ Anti-pattern: new admin client & existence check for every message
                var admin = new ServiceBusAdministrationClient(_connectionString);
                if (!await admin.QueueExistsAsync(_queueName))
                {
                    await admin.CreateQueueAsync(_queueName);
                }

                // ❌ Anti-pattern: create a new client/sender for each message
                await using var client = new ServiceBusClient(_connectionString);
                ServiceBusSender sender = client.CreateSender(_queueName);

                // ❌ Anti-pattern: no batching; one message at a time
                await sender.SendMessageAsync(new ServiceBusMessage(p));

                // ❌ Extra overhead closing every loop; also forces connection churn
                await sender.CloseAsync();
            }
        }
    }
}
