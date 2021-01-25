using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json.Linq;

namespace EventHubsSender
{
    class Program
    {
        
        private static string connectionString = ConfigurationManager.AppSettings["connectionString"];
        private static string fileUrl = ConfigurationManager.AppSettings["fileUrl"];
        private static bool hasHeaders = Boolean.Parse(ConfigurationManager.AppSettings["hasHeaders"]);
        private static int numLinesPerBatch = Int32.Parse(ConfigurationManager.AppSettings["numLinesPerBatch"]);
        private static int sendInterval = Int32.Parse(ConfigurationManager.AppSettings["sendInterval"]);
        private static int timeField = Int32.Parse(ConfigurationManager.AppSettings["timeField"]);
        private static bool setToCurrentTime = Boolean.Parse(ConfigurationManager.AppSettings["setToCurrentTime"]);
        private static string dateFormat = ConfigurationManager.AppSettings["dateFormat"];
        private static bool repeatSimulation = Boolean.Parse(ConfigurationManager.AppSettings["repeatSimulation"]);
        static async Task Main()
        {
            //Console.WriteLine("Starting...");
            try
            {

                HttpWebRequest myHttpWebRequest = (HttpWebRequest)WebRequest.Create(fileUrl);
                // Sends the HttpWebRequest and waits for the response.			
                HttpWebResponse myHttpWebResponse = (HttpWebResponse)myHttpWebRequest.GetResponse();
                // Gets the stream associated with the response.
                Stream receiveStream = myHttpWebResponse.GetResponseStream();
                Encoding encode = System.Text.Encoding.GetEncoding("utf-8");
                // Pipes the stream to a higher level stream reader with the required encoding format. 
                StreamReader readStream = new StreamReader(receiveStream, encode);
                string line;
                string headerLine;
                string[] fields = null;
                JObject schema = null;
                string connectionSubstring = connectionString.Substring(0,connectionString.LastIndexOf(';'));
                Console.WriteLine(connectionSubstring);
                string eventHubName = connectionString.Substring(connectionString.LastIndexOf('=')+1);
                Console.WriteLine(eventHubName);

                // Read and display lines from the file until the end of 
                // the file is reached.
                string[] contentArray = readStream.ReadToEnd().Replace("\r", "").Split('\n');

                readStream.Close();

                int c = contentArray.Length;
                bool runTask = true;


                if (hasHeaders)
                {
                    if ((headerLine = contentArray[0]) != null)
                    {
                        schema = new JObject();
                        fields = headerLine.Split(",");
                        foreach (string fieldName in fields)
                        {
                            schema[fieldName] = null;
                        }
                        Console.WriteLine(schema);
                    }
                }

                //topicClient = new TopicClient(ServiceBusConnectionString, TopicName);
                // Create a producer client that you can use to send events to an event hub
                await using (var producerClient = new EventHubProducerClient(connectionSubstring, eventHubName))
                {
                    int count = 0;
                    int countTotal = 0;
                    EventDataBatch eventBatch = null;
                    //string messageBody = "";
                    
                    var stopwatch = new Stopwatch();
                    while (runTask)
                    {
                        for (int l = (hasHeaders ? 1 : 0); l < c; l++)
                        {
                            line = contentArray[l];

                            // Create a batch of events if needed
                            if (eventBatch == null)
                            {
                                eventBatch = await producerClient.CreateBatchAsync();
                                stopwatch.Start();
                            }
                            eventBatch = eventBatch ?? await producerClient.CreateBatchAsync();
                            dynamic[] values = line.Split(",");
                            for (int i = 0; i < schema.Count; i++)
                            {
                                long longVal = 0;
                                decimal decVal = 0;
                                bool isLong = long.TryParse(values[i], out longVal);
                                bool isDec = decimal.TryParse(values[i], out decVal);
                                schema[fields[i]] = isLong ? longVal : isDec ? decVal : values[i];
                            }
                            if (setToCurrentTime)
                            {
                                if (String.IsNullOrEmpty(dateFormat))
                                {
                                    schema[fields[timeField]] = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds();
                                }
                                else
                                {
                                    schema[fields[timeField]] = DateTime.Now.ToString(dateFormat);
                                }
                            }
                            //Console.WriteLine(schema.ToString());
                            count++;

                            // Add events to the batch. An event is a represented by a collection of bytes and metadata. 

                            eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(schema.ToString())));
                            //messageBody = messageBody + schema.ToString()+"\n";
                            if (count == numLinesPerBatch)
                            {

                                // Use the producer client to send the batch of events to the event hub
                                await producerClient.SendAsync(eventBatch);
                                //var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                                //await topicClient.SendAsync(message);
                                countTotal += count;
                                eventBatch = null;
                                //messageBody = "";
                                stopwatch.Stop();
                                int elapsed_time = (int)stopwatch.ElapsedMilliseconds;
                                stopwatch.Reset();
                                //Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds. Total sent: {2}.", count, elapsed_time, countTotal));
                                if (elapsed_time < sendInterval) {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds. Waiting for {2} milliseconds. Total sent: {3}.", count, elapsed_time, sendInterval - elapsed_time, countTotal));
                                    Thread.Sleep(sendInterval - elapsed_time);
                                }
                                else
                                {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds.  Total sent: {2}.", count, elapsed_time, countTotal));
                                }
                                count = 0;

                            }
                        }
                        Console.WriteLine(string.Format("Reached the end of the simulation file. Repeat is set to {0}", (repeatSimulation)));
                        if (!repeatSimulation)
                        {
                            runTask = false;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Data);
            }
        }
    }
}
