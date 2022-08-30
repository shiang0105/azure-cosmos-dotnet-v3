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

    internal class JohnV3BenchmarkOperation : IBenchmarkOperation
    {
        private readonly Container container;
        private readonly string partitionKeyPath;
        private readonly Dictionary<string, object> sampleJObject;

        private readonly string[] tenantIds;

        private readonly string databaseName;
        private readonly string containerName;

        private static readonly int tenantCount = 1;

        static JohnV3BenchmarkOperation()
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
        public JohnV3BenchmarkOperation(
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

            int id = -1;
            lock (LOCK)
            {
                id = COUNTER;
                COUNTER++;
            }

            tenantIds = new string[tenantCount];
            tenantIds[0] = "you-should-be-deleted-" + id.ToString();
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
            for (int i = 0; i < 0; i++)
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


        private async Task Test()
        {

            // SQL
            string partitionKey = tenantIds[0];

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
                           // Console.WriteLine(total + " !!!!!!!!!!!!!!!!!!");
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
                                    //ResponseMessage itemResponse = await this.container.DeleteItemStreamAsync(items[i].id, new PartitionKey(partitionKey));

                                    /* using (ResponseMessage itemResponse = await this.container.DeleteItemStreamAsync(id, new PartitionKey(tenant)))
                                     {
                                         if (!response.IsSuccessStatusCode)
                                         {
                                             //Handle and log exception
                                             return;
                                         }

                                         double ruCharges = itemResponse.Headers.RequestCharge;

                                         Console.WriteLine(ruCharges + ": RU delete");
                                     }*/
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e.Message);
                                    throw e;
                                }

                                // double ruCharges = itemResponse.Headers.RequestCharge;

                                //Console.WriteLine(ruCharges + ": RU delete");
                            }
                            try
                            {
                                var result = await container.Scripts.ExecuteStoredProcedureAsync<string>("bulkDeletion", new PartitionKey(partitionKey), new[] { lists });
                                Console.WriteLine("result :" + result.StatusCode + ", " + result.RequestCharge);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                                throw e;
                            }
                        }
                    }
                }
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
