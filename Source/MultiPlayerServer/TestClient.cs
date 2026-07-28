using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

// Simple test client to connect, disconnect and reconnect to MultiPlayerServer
namespace MultiPlayerServerTestClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string host = "127.0.0.1";
            int port = 30000;

            if (args.Length > 0) host = args[0];
            if (args.Length > 1) int.TryParse(args[1], out port);

            Console.WriteLine($"Connecting to {host}:{port}...");
            await ConnectCycle(host, port);
        }

        static async Task ConnectCycle(string host, int port)
        {
            var enc = Encoding.Unicode;

            using (var client = new TcpClient())
            {
                await client.ConnectAsync(host, port);
                Console.WriteLine("Connected. Sending simple hello handshake (legacy style)...");

                var stream = client.GetStream();

                // Send a minimal legacy "player" registration. Format in server expects something like "10: PLAYER name "
                string name = "TestPlayer";
                string msg = $"PLAYER {name} ";
                // craft a naive length prefix similar to the server's expectation
                string payload = $" {msg.Length}: {msg}";
                byte[] data = enc.GetBytes(payload);
                await stream.WriteAsync(data, 0, data.Length);
                await stream.FlushAsync();

                Console.WriteLine("Handshake sent. Will keep connection 5s then close...");
                await Task.Delay(5000);
                Console.WriteLine("Closing connection to simulate drop.");
                client.Close();
            }

            Console.WriteLine("Waiting 3s then reconnecting...");
            await Task.Delay(3000);

            using (var client2 = new TcpClient())
            {
                await client2.ConnectAsync(host, port);
                Console.WriteLine("Reconnected. Sending handshake again (same player name)...");
                var stream = client2.GetStream();
                string name = "TestPlayer";
                string msg = $"PLAYER {name} ";
                string payload = $" {msg.Length}: {msg}";
                byte[] data = enc.GetBytes(payload);
                await stream.WriteAsync(data, 0, data.Length);
                await stream.FlushAsync();

                Console.WriteLine("Rejoin handshake sent. Listening for server messages (5s)...");
                await Task.Delay(5000);
                client2.Close();
            }

            Console.WriteLine("Test cycle finished.");
        }
    }
}
