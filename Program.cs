/* Copyright 2021 Esri
 *
 * Licensed under the Apache License Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
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
        private static bool hasHeaderRow = Boolean.Parse(ConfigurationManager.AppSettings["hasHeaderRow"]);
        private static string fieldDelimiter = ConfigurationManager.AppSettings["fieldDelimiter"];
        private static bool convertToJson = Boolean.Parse(ConfigurationManager.AppSettings["convertToJson"]);
        private static int numLinesPerBatch = Int32.Parse(ConfigurationManager.AppSettings["numLinesPerBatch"]);
        private static int sendInterval = Int32.Parse(ConfigurationManager.AppSettings["sendInterval"]);
        private static long realRateMultiplier = Int64.Parse(ConfigurationManager.AppSettings["realRateMultiplier"]);
        private static int timeField = Int32.Parse(ConfigurationManager.AppSettings["timeField"]);
        private static bool setToCurrentTime = Boolean.Parse(ConfigurationManager.AppSettings["setToCurrentTime"]);
        private static string dateFormat = ConfigurationManager.AppSettings["dateFormat"];
        private static CultureInfo dateCulture = CultureInfo.CreateSpecificCulture(ConfigurationManager.AppSettings["dateCulture"]);
        private static bool repeatSimulation = Boolean.Parse(ConfigurationManager.AppSettings["repeatSimulation"]);
        static async Task Main()
        {
            //Console.WriteLine("Starting...");
            try
            {   
                Console.WriteLine($"Fetching and reading file: {fileUrl}");
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
                JObject schema =  new JObject();

                // Read and display lines from the file until the end of 
                // the file is reached.
                string[] contentArray = readStream.ReadToEnd().Replace("\r", "").Split('\n');

                readStream.Close();

                //int c = contentArray.Length;
                bool runTask = true;



                //create a schema of field names if there is a header row or of generic fieldnames if not
                if(convertToJson){
                    if ((headerLine = contentArray[0]) != null)
                    {
                        //schema = new JObject();
                        fields = headerLine.Split(fieldDelimiter);
                        int fieldNum = 1;
                        foreach (string fieldName in fields)
                        {
                            if (hasHeaderRow){
                                schema[fieldName] = null;
                            }
                            else{  
                                string genericFieldName = $"field{fieldNum}";                              
                                schema[genericFieldName] = null;
                            }
                            fieldNum += 1;
                        }
                        Console.WriteLine("Schema created based on the incoming data:");
                        Console.WriteLine(schema);
                        Dictionary<string,string> dictObj = schema.ToObject<Dictionary<string,string>>();
                        dictObj.Keys.CopyTo(fields,0);
                    }
                }
                if (hasHeaderRow){
                    contentArray = contentArray.Where((source, index) => index != 0).ToArray();
                }

                int c = contentArray.Length;
                numLinesPerBatch = sendInterval == -1 ? 1 : numLinesPerBatch;
                    
                string connectionSubstring = connectionString.Substring(0,connectionString.LastIndexOf(';'));
                Console.WriteLine($"Event hub connection string: {connectionSubstring}");
                string eventHubName = connectionString.Substring(connectionString.LastIndexOf('=')+1);
                Console.WriteLine($"Event hub name (entity path): {eventHubName}");

                //topicClient = new TopicClient(ServiceBusConnectionString, TopicName);
                // Create a producer client that you can use to send events to an event hub
                await using (var producerClient = new EventHubProducerClient(connectionSubstring, eventHubName))
                {
                    int count = 0;
                    int countTotal = 0;
                    EventDataBatch eventBatch = null;
                    
                    
                    var stopwatch = new Stopwatch();
                    var taskStopwatch = new Stopwatch();

                    DateTime previousLineDateTime = DateTime.MinValue;
                    DateTime currentLineDateTime = DateTime.MinValue;
                    bool hasCurrentDate = false;
                    long waitTime = -1;
                    string timeString;
                    long timeUnix;

                    while (runTask)
                    {
                        taskStopwatch.Start();
                        for (int l = 0; l < c; l++)
                        {
                            line = contentArray[l];
                            if (String.IsNullOrEmpty(line)){
                                continue;
                            }

                            // Create a batch of events if needed
                            if (eventBatch == null)
                            {
                                eventBatch = await producerClient.CreateBatchAsync();
                                stopwatch.Start();
                            }
                            eventBatch = eventBatch ?? await producerClient.CreateBatchAsync();
                            string[] values = Regex.Split(line, $"{fieldDelimiter}(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                            
                            if (sendInterval == -1)
                            {
                                timeString = values[timeField];
                                hasCurrentDate = false;

                                if (long.TryParse(timeString, out timeUnix))
                                {
                                    currentLineDateTime = timeString.Length == 10 ? DateTimeOffset.FromUnixTimeSeconds(timeUnix).DateTime : DateTimeOffset.FromUnixTimeMilliseconds(timeUnix).DateTime;
                                    hasCurrentDate = true;
                                }
                                else
                                {
                                    hasCurrentDate = DateTime.TryParse(timeString, out currentLineDateTime);
                                }

                                if (hasCurrentDate && previousLineDateTime != DateTime.MinValue && currentLineDateTime > previousLineDateTime)
                                {
                                    waitTime = Convert.ToInt64((currentLineDateTime - previousLineDateTime).TotalMilliseconds);
                                    Thread.Sleep((int)(waitTime / (realRateMultiplier / 100)));
                                }
                                if (hasCurrentDate)
                                {
                                    previousLineDateTime = currentLineDateTime;
                                }
                            }

                            if (setToCurrentTime)
                            {
                                if (String.IsNullOrEmpty(dateFormat))
                                {
                                    //Console.WriteLine("setting time value");
                                    string dt = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds().ToString();
                                    values[timeField] = dt;
                                    //Console.WriteLine("done setting time value");
                                }
                                else
                                {
                                    try{
                                        string dt = DateTime.Now.ToString(dateFormat,dateCulture);
                                        values[timeField] = dt;
                                    }
                                    catch(Exception e){
                                        string dt = new DateTimeOffset(DateTime.Now).ToUnixTimeMilliseconds().ToString();
                                        values[timeField] = dt;
                                    }
                                }
                            }                            
                                                       

                            if(convertToJson){
                                for (int i = 0; i < schema.Count; i++)
                                { 
                                    long longVal = 0;
                                    decimal decVal = 0;
                                    bool isLong = long.TryParse(values[i], out longVal);
                                    bool isDec = decimal.TryParse(values[i], out decVal);
                                    schema[fields[i]] = isLong ? longVal : isDec ? decVal : values[i];
                                }
                                //Console.WriteLine($"Schema: {schema}");
                            }

                            count++;
                            countTotal++;

                            // Add events to the batch. An event is a represented by a collection of bytes and metadata. 

                            if (convertToJson){
                                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(schema.ToString())));
                            }
                            else{
                                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(string.Join(fieldDelimiter, values))));
                            }
                            
                            //messageBody = messageBody + schema.ToString()+"\n";
                            if (count == numLinesPerBatch || countTotal == c)
                            {

                                // Use the producer client to send the batch of events to the event hub
                                await producerClient.SendAsync(eventBatch);
                                //var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                                //await topicClient.SendAsync(message);
                                //countTotal += count;
                                eventBatch = null;
                                //messageBody = "";
                                stopwatch.Stop();
                                int elapsed_time = (int)stopwatch.ElapsedMilliseconds;
                                stopwatch.Reset();
                                //Console.WriteLine(string.Format("A batch of {0} events has been published. It took {1} milliseconds. Total sent: {2}.", count, elapsed_time, countTotal));
                                if (elapsed_time < sendInterval) {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published in {1}ms. Waiting for {2}ms. Total sent: {3}. Total elapsed time: {4}ms", count, elapsed_time, sendInterval - elapsed_time, countTotal,(int)taskStopwatch.ElapsedMilliseconds));
                                    Thread.Sleep(sendInterval - elapsed_time);
                                }
                                else
                                {
                                    Console.WriteLine(string.Format("A batch of {0} events has been published in {1}ms.  Total sent: {2}. Total elapsed time: {3}ms", count, elapsed_time, countTotal,(int)taskStopwatch.ElapsedMilliseconds));
                                }
                                count = 0;

                            }
                        }
                        Console.WriteLine(string.Format($"Reached the end of the simulation file. Total sent: {countTotal}. Repeat is set to {repeatSimulation}."));
                        if (!repeatSimulation)
                        {
                            runTask = false;
                            taskStopwatch.Stop();                            
                            Console.WriteLine($"Total task duration: {(int)taskStopwatch.ElapsedMilliseconds}ms");
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
