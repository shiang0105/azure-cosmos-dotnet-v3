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

    internal class JohnSeqDeleteV3BenchmarkOperation : IBenchmarkOperation
    {
        private readonly Container container;
        private readonly string partitionKeyPath;
        private readonly Dictionary<string, object> sampleJObject;

        private readonly string[] tenantIds;

        private readonly string databaseName;
        private readonly string containerName;

        private static readonly int tenantCount = 1;

        static JohnSeqDeleteV3BenchmarkOperation()
        {
            /*
             tenantIds = new string[tenantCount];

             for (int i = 0; i < tenantIds.Length; i++)
             {
                 string tenantId = new string('g', 36 - (i.ToString().Length + 1)) + "-" + i.ToString();
                 tenantIds[i] = "you-should-be-deleted";
             }

             */
        }
        public JohnSeqDeleteV3BenchmarkOperation(
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





            await Test();


            return new OperationResult()
            {
                DatabseName = databaseName,
                ContainerName = containerName,
                RuCharges = 0
            };
        }


        private async Task Test()
        {

            // SQL
            string partitionKey = currentTenantId;
            Console.WriteLine(currentTenantId);

            using (FeedIterator setIterator = container.GetItemQueryStreamIterator(
                $"SELECT * FROM c where c.tenantId = \"{partitionKey}\"",
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = new PartitionKey(partitionKey),
                    MaxConcurrency = 1,
                    MaxItemCount = 100
                }))
            {
                int count = 0;
                while (setIterator.HasMoreResults)
                {
                    using (ResponseMessage response = await setIterator.ReadNextAsync())
                    {
                        Console.WriteLine("RB code: " + response.IsSuccessStatusCode + ", " + response.Headers.RequestCharge);

                        //  response.Content.Seek(0, SeekOrigin.Begin);
                        //  using StreamReader ss= new StreamReader(response.Content);
                        //  Console.WriteLine("!!" + ss.ReadToEnd());


                        var lists = new List<string>();

                        count++;
                        using (StreamReader sr = new StreamReader(response.Content))
                        using (JsonTextReader jtr = new JsonTextReader(sr))
                        {
                            JsonSerializer jsonSerializer = new JsonSerializer();
                            dynamic root = jsonSerializer.Deserialize<dynamic>(jtr);
                            dynamic items = root.Documents;
                            int total = root._count;
                            Console.WriteLine(total + " !!!!!!!!!!!!!!!!!!");
                            for (int i = 0; i < total; i++)
                            {
                                //Console.WriteLine(items[i]._self);
                                //Console.WriteLine(items[i].id);
                                //Console.WriteLine(partitionKey);
                                //Console.WriteLine(items[i].tenantId);
                                try
                                {
                                    string id = items[i].id;
                                    string tenant = items[i].tenantId;
                                    string link = items[i]._self;
                                    lists.Add(link);

                                    using (ResponseMessage itemResponse = await this.container.DeleteItemStreamAsync(id, new PartitionKey(tenant)))
                                    {
                                        if (!response.IsSuccessStatusCode)
                                        {
                                            throw new Exception("failed to delete");
                                        }

                                        double ruCharges = itemResponse.Headers.RequestCharge;

                                       // Console.WriteLine(ruCharges + ": RU delete");
                                    }
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e.Message);
                                    throw e;
                                }

                                // double ruCharges = itemResponse.Headers.RequestCharge;

                                //Console.WriteLine(ruCharges + ": RU delete");
                            }
                        }
                    }
                }
            }
        }


        string currentTenantId = "";
        public Task PrepareAsync()
        {
            Random rnd = new Random();
            int id = -1;
            lock (LOCK)
            {
                id = COUNTER;
                COUNTER++;
            }

            currentTenantId = "you-should-be-deleted-" + id.ToString();

            return Task.CompletedTask;
        }
    }
}
