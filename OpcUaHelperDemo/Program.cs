using System;
using System.Collections.Generic;
using Opc.Ua.UaHelper;
using Opc.Ua.Client;
using Opc.Ua;

namespace OpcUaHelperDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            OpcUaClient client = new OpcUaClient();
            client.Connect("opc.tcp://127.0.0.1:49320");

            Console.WriteLine("单点读");
            Console.WriteLine("ns=2;s=Channel1.Device1.Tag1");
            var retValue =  client.ReadNode("ns=2;s=Channel1.Device1.Tag1");
            Console.WriteLine(retValue);

            Console.WriteLine("单点写");
            client.WriteNode("ns=2;s=Channel1.Device1.Tag2", "111");
            Console.WriteLine("ns=2;s=Channel1.Device1.Tag2");
            retValue = client.ReadNode("ns=2;s=Channel1.Device1.Tag2");
            Console.WriteLine(retValue);

            Console.WriteLine("多点读");
            var retValues = client.ReadNodes(new List<string> { "ns=2;s=Channel1.Device1.Tag1", "ns=2;s=Channel1.Device1.Tag2" });
            retValues.ForEach(x => Console.WriteLine(x));

            Console.WriteLine("多点写");
            client.WriteNodes(new List<string> { "ns=2;s=Channel1.Device1.Tag1", "ns=2;s=Channel1.Device1.Tag2" },new List<string> { "100","100"});
            retValues = client.ReadNodes(new List<string> { "ns=2;s=Channel1.Device1.Tag1", "ns=2;s=Channel1.Device1.Tag2" });
            retValues.ForEach(x => Console.WriteLine(x));

            client.AddSubscription("monitor", "ns=2;s=Channel1.Device1.Tag1", (string key, MonitoredItem monitoredItem, MonitoredItemNotificationEventArgs EventArgs) => {
                Console.WriteLine(key + ":" + monitoredItem.StartNodeId + "->" + (EventArgs.NotificationValue as MonitoredItemNotification).Value.ToString());
            });


            Console.ReadKey();
        }
    }
}
