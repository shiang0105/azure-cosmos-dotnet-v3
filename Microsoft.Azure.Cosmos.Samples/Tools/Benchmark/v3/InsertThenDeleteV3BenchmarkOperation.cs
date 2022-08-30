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
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json.Serialization;

    internal class InsertThenDeleteV3BenchmarkOperation : IBenchmarkOperation
    {
        private readonly Container container;
        private readonly string partitionKeyPath;
        private readonly Dictionary<string, object> sampleJObject;

        private static readonly string[] tenantIds;

        private readonly string databaseName;
        private readonly string containerName;

        private static readonly int tenantCount = 1;

        static InsertThenDeleteV3BenchmarkOperation()
        {
            tenantIds = new string[tenantCount];

            for (int i = 0; i < tenantIds.Length; i++)
            {
                string tenantId = new string('x', 36 - (i.ToString().Length + 1)) + "-" + i.ToString();
                tenantIds[i] = tenantId;
            }
        }
        public InsertThenDeleteV3BenchmarkOperation(
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

        public async Task<OperationResult> ExecuteOnceAsync()
        {


            using (MemoryStream input = JsonHelper.ToStream(this.sampleJObject))
            {

                ResponseMessage itemResponse = await this.container.CreateItemStreamAsync(
                        input,
                        new PartitionKey(this.sampleJObject[this.partitionKeyPath].ToString()));

                double ruCharges = itemResponse.Headers.RequestCharge;
                double deleteRu = 1000000;

                System.Buffers.ArrayPool<byte>.Shared.Return(input.GetBuffer());


                itemResponse.Content.Seek(0, SeekOrigin.Begin);

                using (StreamReader sr = new StreamReader(itemResponse.Content))
                using (JsonTextReader jtr = new JsonTextReader(sr))
                {
                    JsonSerializer jsonSerializer = new JsonSerializer();
                    dynamic root = jsonSerializer.Deserialize<dynamic>(jtr);

                    string id = root.id;
                    string tenantId = root.tenantId;

                    using (ResponseMessage deleteResponse = await this.container.DeleteItemStreamAsync(id, new PartitionKey(tenantId)))
                    {
                        if (!deleteResponse.IsSuccessStatusCode)
                        {
                            //Handle and log exception
                            throw new Exception("fail to delete");
                        }

                        deleteRu = deleteResponse.Headers.RequestCharge;
                    }
                }

                return new OperationResult()
                {
                    DatabseName = databaseName,
                    ContainerName = containerName,
                    RuCharges = ruCharges + deleteRu,
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
