// 
// Code forked from Open Rails Ultimate (now FreeTrainSimulator)
//
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace MultiPlayerServer
{
    // Simple disconnected session holder for reconnection window
    internal class DisconnectedSession
    {
        public DateTime DisconnectedAt { get; set; }
        public byte[] LastKnownBuffer { get; set; }
        public string SessionToken { get; set; }
    }

    public class Host
    {
        private readonly int port;

        private static readonly Encoding encoding = Encoding.Unicode;
        private static readonly int charSize = encoding.GetByteCount("0");

        // thread-safe collection of currently online players
        private readonly ConcurrentDictionary<string, TcpClient> onlinePlayers = new ConcurrentDictionary<string, TcpClient>();

        // temporarily keep disconnected sessions to allow fast rejoin
        private readonly ConcurrentDictionary<string, DisconnectedSession> disconnectedSessions = new ConcurrentDictionary<string, DisconnectedSession>();

        private static readonly byte[] initData = encoding.GetBytes("10: SERVER YOU");
        private static readonly byte[] serverChallenge = encoding.GetBytes(" 21: SERVER WhoCanBeServer");
        private static readonly byte[] blankToken = encoding.GetBytes(" ");
        private static readonly byte[] playerToken = encoding.GetBytes(": PLAYER ");
        private static readonly byte[] quitToken = encoding.GetBytes(": QUIT ");
        private string currentServer;

        // reconnection window in seconds (configurable)
        private readonly TimeSpan ReconnectWindow = TimeSpan.FromSeconds(30);

        public Host(int port)
        {
            this.port = port;
        }

        public async Task Run()
        {
            try
            {
                TcpListener listener = new TcpListener(IPAddress.Any, port);
                listener.Start();
#pragma warning disable CA1303 // Do not pass literals as localized parameters
                Console.WriteLine($"MultiPlayer Server is now running on port {port}");
                Console.WriteLine("Taken from OR Ultimate (now FreeTrainSimulator)");
                Console.WriteLine("Hit <enter> to stop service");
                Console.WriteLine();
#pragma warning restore CA1303 // Do not pass literals as localized parameters
                while (true)
                {
                    try
                    {
                        Pipe pipe = new Pipe();

                        TcpClient tcpClient = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                        _ = PipeFillAsync(tcpClient, pipe.Writer);
                        _ = PipeReadAsync(tcpClient, pipe.Reader);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        throw new InvalidOperationException("Invalid Program state, aborting.", ex);
                    }
                }
            }
            catch (SocketException socketException)
            {
                Console.WriteLine(socketException.Message);
                throw;
            }
        }

        private async Task PipeFillAsync(TcpClient tcpClient, PipeWriter writer)
        {
            const int minimumBufferSize = 1024;
            _ = currentServer;
            NetworkStream networkStream = tcpClient.GetStream();

            while (tcpClient.Connected)
            {
                Memory<byte> memory = writer.GetMemory(minimumBufferSize);

                int bytesRead = await networkStream.ReadAsync(memory).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    break;
                }
                writer.Advance(bytesRead);

                FlushResult result = await writer.FlushAsync().ConfigureAwait(false);

                if (result.IsCompleted)
                {
                    break;
                }
            }
            await writer.CompleteAsync().ConfigureAwait(false);
        }

        private bool ReadPlayerName(in ReadOnlySequence<byte> sequence, ref string playerName, out SequencePosition bytesProcessed, out ReadOnlySequence<byte> pendingPlayerMessage)
        {
            // This method attempts to keep the original parsing behavior but returns any playerMessage
            // so the async caller can handle sending instead of blocking here.
            pendingPlayerMessage = ReadOnlySequence<byte>.Empty;

            Span<byte> playerSeparator = playerToken.AsSpan();
            Span<byte> blankSeparator = blankToken.AsSpan();

            SequenceReader<byte> reader = new SequenceReader<byte>(sequence);

            if (reader.TryReadTo(out ReadOnlySequence<byte> playerPreface, playerSeparator))
            {
                if (reader.TryReadTo(out ReadOnlySequence<byte> playerNameSequence, blankSeparator))
                {
                    int maxDigits = 4;
                    if (playerPreface.GetIntFromEnd(ref maxDigits, out int length, encoding))
                    {
                        ReadOnlySequence<byte> before = sequence.Slice(0, playerPreface.Length - maxDigits * charSize);
                        foreach (ReadOnlyMemory<byte> message in before)
                        {
                            if (message.Length > 0)
                                BroadcastAsync(playerName, message).ConfigureAwait(false); // fire and forget for preface
                        }
                        reader.Rewind(playerSeparator.Length + playerNameSequence.Length + maxDigits * charSize);

                        if (reader.Remaining >= length * charSize)
                        {
                            string newPlayerName = playerNameSequence.GetString(encoding);
                            ReadOnlySequence<byte> playerMessage = reader.Sequence.Slice(before.Length, (length + maxDigits + 2) * charSize);

                            // Return the playerMessage to the caller so it can be sent asynchronously to currentServer
                            pendingPlayerMessage = playerMessage;

                            playerName = newPlayerName;
                            bytesProcessed = sequence.GetPosition(before.Length + playerMessage.Length);
                            return true;
                        }
                    }
                }
            }
            bytesProcessed = sequence.GetPosition(sequence.Length);
            return false;
        }

        private static string ReadQuitMessage(ReadOnlySequence<byte> sequence)
        {
            Span<byte> quitSeparator = quitToken.AsSpan();
            Span<byte> blankSeparator = blankToken.AsSpan();

            SequenceReader<byte> reader = new SequenceReader<byte>(sequence);

            if (reader.TryReadTo(out ReadOnlySequence<byte> _, quitSeparator))
            {
                if (reader.TryReadTo(out ReadOnlySequence<byte> playerName, blankSeparator))
                {
                    return playerName.GetString(encoding);
                }
            }
            return null;
        }

        private async Task PipeReadAsync(TcpClient tcpClient, PipeReader reader)
        {
            string playerName = tcpClient.Client.RemoteEndPoint.ToString();
            bool playerNameSet = false;
            string quitPlayer;
            onlinePlayers.TryAdd(playerName, tcpClient);
            if (onlinePlayers.Count == 1)
            {
                currentServer = playerName;
                await SendMessage(playerName, initData).ConfigureAwait(false);
            }

            while (tcpClient.Client.Connected)
            {
                ReadResult result = await reader.ReadAsync().ConfigureAwait(false);

                ReadOnlySequence<byte> buffer = result.Buffer;

                if (!playerNameSet)
                {
                    string player = playerName;
                    if (ReadPlayerName(buffer, ref player, out SequencePosition bytesProcessed, out ReadOnlySequence<byte> pendingPlayerMessage))
                    {
                        // if there is a disconnected session for this player, try to reattach
                        if (disconnectedSessions.TryRemove(player, out var dsession))
                        {
                            Console.WriteLine($"Player {player} rejoined within window. Restoring session.");
                            // remove any old entry keyed by the new name and add new tcp client
                            onlinePlayers.TryRemove(playerName, out _);
                            onlinePlayers.TryAdd(player, tcpClient);

                            // notify other players that this player is back
                            var rejoinMsg = encoding.GetBytes($" {("REJOINED " + player).Length}: {"REJOINED " + player}");
                            await BroadcastAsync(null, rejoinMsg).ConfigureAwait(false);

                            // ask the current server to send a state snapshot to this player
                            if (!string.IsNullOrEmpty(currentServer) && currentServer != player)
                            {
                                var requestState = encoding.GetBytes($" {("REQUEST_STATE " + player).Length}: {"REQUEST_STATE " + player}");
                                await SendMessage(currentServer, requestState).ConfigureAwait(false);
                            }

                            playerNameSet = true;
                            playerName = player;
                        }
                        else
                        {
                            // Normal new-player registration
                            onlinePlayers.TryRemove(playerName, out _);
                            if (currentServer == playerName)
                                currentServer = playerName = player;
                            else
                                playerName = player;
                            onlinePlayers.TryAdd(playerName, tcpClient);
                            playerNameSet = true;

                            // if there is a pendingPlayerMessage (from the buffered handshake), forward it to current server asynchronously
                            if (pendingPlayerMessage.Length > 0 && !string.IsNullOrEmpty(currentServer) && currentServer != playerName)
                            {
                                foreach (ReadOnlyMemory<byte> message in pendingPlayerMessage)
                                {
                                    // send without blocking the reader loop; errors handled inside SendMessage
                                    _ = SendMessage(currentServer, message);
                                }
                            }
                        }
                    }
                    reader.AdvanceTo(bytesProcessed);
                }
                else
                {
                    if (!string.IsNullOrEmpty(quitPlayer = ReadQuitMessage(buffer)) && playerName == quitPlayer)
                        break;

                    // cache last buffer for quick restore during short disconnects
                    if (buffer.Length > 0)
                    {
                        try
                        {
                            var copy = new byte[buffer.Length];
                            buffer.CopyTo(copy);
                            disconnectedSessions.AddOrUpdate(playerName, new DisconnectedSession { DisconnectedAt = DateTime.UtcNow, LastKnownBuffer = copy }, (k, v) => { v.LastKnownBuffer = copy; v.DisconnectedAt = DateTime.UtcNow; return v; });
                        }
                        catch { }
                    }

                    foreach (ReadOnlyMemory<byte> message in buffer)
                    {
                        // propagate message to others
                        await BroadcastAsync(playerName, message).ConfigureAwait(false);
                    }
                    reader.AdvanceTo(buffer.End);
                }

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await RemovePlayer(playerName).ConfigureAwait(false);

            await reader.CompleteAsync().ConfigureAwait(false);
        }

        private async Task BroadcastAsync(string playerName, ReadOnlyMemory<byte> buffer)
        {
            Console.WriteLine(encoding.GetString(buffer.Span).Replace("\r", Environment.NewLine, StringComparison.OrdinalIgnoreCase));

            var targets = onlinePlayers.Where(kv => kv.Key != playerName).ToArray();
            var tasks = new List<Task>(targets.Length);

            foreach (var kv in targets)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        TcpClient client = kv.Value;
                        NetworkStream clientStream = client.GetStream();
                        await clientStream.WriteAsync(buffer).ConfigureAwait(false);
                        await clientStream.FlushAsync().ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is System.IO.IOException || ex is SocketException || ex is InvalidOperationException)
                    {
                        // If sending fails, schedule removal of that player
                        Console.WriteLine($"Broadcast failed to {kv.Key}: {ex.Message}");
                        await RemovePlayer(kv.Key).ConfigureAwait(false);
                    }
                }));
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch { /* individual send errors handled above */ }
        }

        private async Task SendMessage(string playerName, ReadOnlyMemory<byte> buffer)
        {
            Console.WriteLine(encoding.GetString(buffer.Span).Replace("\r", Environment.NewLine, StringComparison.OrdinalIgnoreCase));
            try
            {
                if (playerName == null)
                {
                    // broadcast to everyone
                    await BroadcastAsync(null, buffer).ConfigureAwait(false);
                    return;
                }

                if (onlinePlayers.TryGetValue(playerName, out var client))
                {
                    NetworkStream clientStream = client.GetStream();
                    await clientStream.WriteAsync(buffer).ConfigureAwait(false);
                    await clientStream.FlushAsync().ConfigureAwait(false);
                }
                else
                {
                    // player not online - ignore or keep for later
                    Console.WriteLine($"Attempt to send to offline player {playerName}");
                }
            }
            catch (Exception ex) when (ex is System.IO.IOException || ex is SocketException || ex is InvalidOperationException)
            {
                if (playerName != null)
                    await RemovePlayer(playerName).ConfigureAwait(false);
            }
        }

        private async Task RemovePlayer(string playerName)
        {
            if (string.IsNullOrEmpty(playerName)) return;

            if (onlinePlayers.TryRemove(playerName, out var removedClient))
            {
                // store a short-lived disconnected session to allow quick rejoin
                try
                {
                    var dsession = new DisconnectedSession { DisconnectedAt = DateTime.UtcNow };
                    disconnectedSessions[playerName] = dsession;

                    // Broadcast that the player was lost (keep compatibility)
                    string lostMessage = $"LOST { playerName}";
                    byte[] lostPlayer = encoding.GetBytes($" {lostMessage.Length}: {lostMessage}");
                    await BroadcastAsync(playerName, lostPlayer).ConfigureAwait(false);

                    // If player was current server, start server re-election flow
                    if (currentServer == playerName)
                    {
                        await BroadcastAsync(playerName, serverChallenge).ConfigureAwait(false);
                        await Task.Delay(5000).ConfigureAwait(false);
                        if (onlinePlayers.Count > 0)
                        {
                            // appoint first remaining
                            currentServer = onlinePlayers.Keys.First();
                            string appointmentMessage = $"SERVER {currentServer}";
                            lostPlayer = encoding.GetBytes($" {appointmentMessage.Length}: {appointmentMessage}");
                            await BroadcastAsync(null, lostPlayer).ConfigureAwait(false);
                        }
                    }

                    // schedule cleanup after ReconnectWindow
                    _ = Task.Run(async () =>
                    {
                        await Task.Delay(ReconnectWindow).ConfigureAwait(false);
                        if (disconnectedSessions.TryGetValue(playerName, out var session) && DateTime.UtcNow - session.DisconnectedAt >= ReconnectWindow)
                        {
                            disconnectedSessions.TryRemove(playerName, out _);
                            Console.WriteLine($"Session for {playerName} expired and was removed.");
                        }
                    });
                }
                catch { }
                finally
                {
                    try { removedClient?.Close(); } catch { }
                }
            }
        }
    }

    public static class ReadOnlySequenceExtensions
    {
        public static bool GetIntFromEnd(in this ReadOnlySequence<byte> payload, ref int maxDigits, out int result, Encoding encoding = null)
        {
            if (encoding == null) encoding = Encoding.UTF8;
            int charSize = encoding.GetByteCount("0");

            if (maxDigits > 0)
            {
                if (maxDigits * charSize > payload.Length)
                    maxDigits = (int)payload.Length / charSize;
                SequencePosition position = payload.GetPosition(payload.Length - maxDigits * charSize);
                if (payload.TryGet(ref position, out ReadOnlyMemory<byte> lengthIndicator, false))
                {
                    if (int.TryParse(encoding.GetString(lengthIndicator.Span), out result))
                        return true;
                    else
                    {
                        maxDigits--;
                        return GetIntFromEnd(payload, ref maxDigits, out result, encoding);
                    }
                }
            }
            result = 0;
            return false;
        }

        public static string GetString(in this ReadOnlySequence<byte> payload, Encoding encoding = null)
        {
            if (encoding == null) encoding = Encoding.UTF8;

            return payload.IsSingleSegment ? encoding.GetString(payload.FirstSpan)
                : GetStringInternal(payload, encoding);

            static string GetStringInternal(in ReadOnlySequence<byte> payload, Encoding encoding)
            {
                // linearize
                int length = checked((int)payload.Length);
                byte[] oversized = ArrayPool<byte>.Shared.Rent(length);
                try
                {
                    payload.CopyTo(oversized);
                    return encoding.GetString(oversized, 0, length);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(oversized);
                }
            }
        }
    }

}
