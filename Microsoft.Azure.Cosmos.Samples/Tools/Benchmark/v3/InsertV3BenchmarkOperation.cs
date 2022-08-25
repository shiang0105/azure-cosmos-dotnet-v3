//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace CosmosBenchmark
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    internal class InsertV3BenchmarkOperation : IBenchmarkOperation
    {
        private readonly Container container;
        private readonly string partitionKeyPath;
        private readonly Dictionary<string, object> sampleJObject;

        private static readonly string[] tenantIds;

        private readonly string databaseName;
        private readonly string containerName;

        private static readonly int tenantCount = 5000;

        static InsertV3BenchmarkOperation()
        {
            tenantIds = new string[tenantCount];

            for (int i = 0; i < tenantIds.Length; i++)
            {
                tenantIds[i] = Guid.NewGuid().ToString();
            }
        }
        public InsertV3BenchmarkOperation(
            CosmosClient cosmosClient,
            string dbName,
            string containerName,
            string partitionKeyPath,
            string sampleJson)
        {
            this.databaseName = dbName;
            this.containerName = containerName;

            this.container = cosmosClient.GetContainer(this.databaseName, this.containerName);
            this.partitionKeyPath = partitionKeyPath.Replace("/", "");

            this.sampleJObject = JsonHelper.Deserialize<Dictionary<string, object>>(sampleJson);
        }

        public static int cnt1 = 0;

        public static int cnt = 0;

        public async Task<OperationResult> ExecuteOnceAsync()
        {
            if (cnt1++ < 2)
            {
                Console.WriteLine(partitionKeyPath + "----------------------------");
                foreach (var k in sampleJObject)
                {
                    Console.WriteLine(k.Key + ":" + k.Value);
                }
                Console.WriteLine(partitionKeyPath + "----------------------------===========================");
            }
            using (MemoryStream input = JsonHelper.ToStream(this.sampleJObject))
            {
                if (cnt++ < 2)
                {
                    Console.WriteLine(Encoding.ASCII.GetString(input.ToArray()));
                    input.Position = 0;
                }
                ResponseMessage itemResponse = await this.container.CreateItemStreamAsync(
                        input,
                        new PartitionKey(this.sampleJObject[this.partitionKeyPath].ToString()));

                double ruCharges = itemResponse.Headers.RequestCharge;

                System.Buffers.ArrayPool<byte>.Shared.Return(input.GetBuffer());

                return new OperationResult()
                {
                    DatabseName = databaseName,
                    ContainerName = containerName,
                    RuCharges = ruCharges,
                    CosmosDiagnostics = itemResponse.Diagnostics,
                    LazyDiagnostics = () => itemResponse.Diagnostics.ToString(),
                };
            }
        }

        public Task PrepareAsync()
        {
            Random rnd = new Random();
            string tenantId = tenantIds[rnd.Next(tenantCount)];
            this.sampleJObject["id"] = Guid.NewGuid().ToString();
            this.sampleJObject[this.partitionKeyPath] = tenantId;

            return Task.CompletedTask;
        }
    }
}
