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

    internal class JohnInsertV3BenchmarkOperation : IBenchmarkOperation
    {
        private readonly Container container;
        private readonly string partitionKeyPath;
        private readonly Dictionary<string, object> sampleJObject;


        private readonly string databaseName;
        private readonly string containerName;



        public JohnInsertV3BenchmarkOperation(
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

        public static object LOCK = new object();

        public static int COUNTER = 0;

        private int id = 0;

        public static int cnt1 = 0;

        public static int cnt = 0;
        bool insertMode = true;
        public async Task<OperationResult> ExecuteOnceAsync()
        {


            Console.WriteLine(this.sampleJObject[this.partitionKeyPath].ToString());
            for (int i = 0; i < 5; i++)
            {

                this.sampleJObject["id"] = Guid.NewGuid().ToString();


                using (MemoryStream input = JsonHelper.ToStream(this.sampleJObject))
                {

                    ResponseMessage itemResponse = await this.container.CreateItemStreamAsync(
                            input,
                            new PartitionKey(this.sampleJObject[this.partitionKeyPath].ToString()));

                    double ruCharges = itemResponse.Headers.RequestCharge;

                    System.Buffers.ArrayPool<byte>.Shared.Return(input.GetBuffer());


                    itemResponse.Content.Seek(0, SeekOrigin.Begin);

                    using (StreamReader sr = new StreamReader(itemResponse.Content))
                    using (JsonTextReader jtr = new JsonTextReader(sr))
                    {
                        JsonSerializer jsonSerializer = new JsonSerializer();
                        dynamic root = jsonSerializer.Deserialize<dynamic>(jtr);

                        string self = root._self;
                        //Console.WriteLine(ruCharges + ": RU");
                    }


                    /* return new OperationResult()
                     {
                         DatabseName = databaseName,
                         ContainerName = containerName,
                         RuCharges = ruCharges,
                         CosmosDiagnostics = itemResponse.Diagnostics,
                         LazyDiagnostics = () => itemResponse.Diagnostics.ToString(),
                     };*/
                }
            }



            //   await Test();


            return new OperationResult()
            {
                DatabseName = databaseName,
                ContainerName = containerName,
                RuCharges = 0
            };
        }

        public Task PrepareAsync()
        {
            int id = -1;
            lock (LOCK)
            {
                id = COUNTER;
                COUNTER++;
            }

            this.sampleJObject["id"] = Guid.NewGuid().ToString();
            this.sampleJObject[this.partitionKeyPath] = "you-should-be-deleted-" + id.ToString();

            return Task.CompletedTask;
        }
    }
}
