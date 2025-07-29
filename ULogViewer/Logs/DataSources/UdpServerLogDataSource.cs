using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CarinaStudio.ULogViewer.Logs.DataSources
{
    /// <summary>
	/// <see cref="ILogDataSource"/> based-on <see cref="UdpClient"/>.
	/// </summary>
    class UdpServerLogDataSource : BaseLogDataSource
    {
        // Implmentation of text reader.
        class ReaderImpl : TextReader
        {
            // Fields.
            readonly Encoding encoding;
            volatile bool hasNewLine;
            volatile bool isDisposed;
            readonly object readingLock = new();
            readonly UdpClient udpClient;
            readonly IPEndPoint serverEndPoint;
            readonly UdpServerLogDataSource source;
            readonly StringBuilder stringBuffer = new();
            Task? receivingTask;

            // Constructor.
            public ReaderImpl(UdpServerLogDataSource source, UdpClient udpClient, IPEndPoint serverEndPoint, Encoding encoding)
            {
                this.encoding = encoding;
                this.serverEndPoint = serverEndPoint;
                this.source = source;
                this.udpClient = udpClient;
                this.receivingTask = Task.Run(() => this.ReceiveLoopAsync());
            }

            // Dispose.
            protected override void Dispose(bool disposing)
            {
                lock (this.readingLock)
                {
                    if (this.isDisposed)
                        return;
                    this.isDisposed = true;
                    Monitor.PulseAll(this.readingLock);
                }
                this.source.Logger.LogWarning("Stop receiving from {serverEndPoint}", this.serverEndPoint);
                Global.RunWithoutError(() =>
                {
                    this.udpClient.Dispose();
                });
                if (this.receivingTask != null)
                {
                    try
                    {
                        this.receivingTask.Wait(500);
                    }
                    catch
                    {
                        // Ignore any exceptions during task wait
                    }
                    this.receivingTask = null;
                }
                this.stringBuffer.Remove(0, this.stringBuffer.Length);
                base.Dispose(disposing);
            }

            // Async receive loop
            async Task ReceiveLoopAsync()
            {
                while (!this.isDisposed)
                {
                    try
                    {
                        var result = await this.udpClient.ReceiveAsync();
                        var data = result.Buffer;
                        var s = this.encoding.GetString(data);
                        if (!string.IsNullOrEmpty(s))
                        {
                            lock (readingLock)
                            {
                                if (this.isDisposed)
                                    break;
                                this.stringBuffer.Append(s);
                                this.hasNewLine = true;
                                Monitor.PulseAll(readingLock);
                            }
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Socket has been disposed - normal during shutdown
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (this.isDisposed)
                            break;
                        this.source.Logger.LogError(ex, "Error receiving data from {serverEndPoint}", this.serverEndPoint);
                    }
                }
            }

            // Implementations.
            public override string? ReadLine()
            {
                lock (this.readingLock)
                {
                    while (true)
                    {
                        // check state
                        if (this.isDisposed)
                            return null;

                        // wait for new line
                        if (!this.hasNewLine)
                        {
                            Monitor.Wait(this.readingLock);
                            continue;
                        }

                        // find line break
                        var stringBuffer = this.stringBuffer;
                        var bufferLength = stringBuffer.Length;
                        var endIndex = 0;
                        while (endIndex < bufferLength)
                        {
                            var c = stringBuffer[endIndex];
                            if (c == '\n' || c == '\r')
                                break;
                            ++endIndex;
                        }
                        var removingEndIndex = endIndex + 1;
                        while (removingEndIndex < bufferLength)
                        {
                            var c = stringBuffer[removingEndIndex];
                            if (c != '\n' && c != '\r')
                                break;
                            ++removingEndIndex;
                        }

                        // read line
                        var line = Global.Run(() =>
                        {
                            if (endIndex > 0)
                            {
                                var chars = new char[endIndex];
                                stringBuffer.CopyTo(0, chars, 0, endIndex);
                                return new string(chars);
                            }
                            else
                                return "";
                        });
                        stringBuffer.Remove(0, removingEndIndex);
                        hasNewLine = stringBuffer.Length > 0;
                        return line;
                    }
                }
            }
        }


        /// <summary>
        /// Initialize new <see cref="UdpServerLogDataSource"/> instance.
        /// </summary>
        /// <param name="provider">Provider.</param>
        /// <param name="options">Options.</param>
        public UdpServerLogDataSource(UdpServerLogDataSourceProvider provider, LogDataSourceOptions options) : base(provider, options)
        {
            if (options.IPEndPoint == null)
                throw new ArgumentException("No IP endpoint specified.");
        }


        // Open reader.
        protected override Task<(LogDataSourceState, TextReader?)> OpenReaderCoreAsync(CancellationToken cancellationToken)
        {
            // get options
            var options = this.CreationOptions;
            var encoding = options.Encoding ?? Encoding.UTF8;
            var endPoint = options.IPEndPoint.AsNonNull();

            // create UDP server
            var udpClient = new UdpClient(endPoint);

            // create reader
            return Task.FromResult<(LogDataSourceState, TextReader?)>((LogDataSourceState.ReaderOpened, new ReaderImpl(this, udpClient, endPoint, encoding)));
        }


        // Prepare.
        protected override Task<LogDataSourceState> PrepareCoreAsync(CancellationToken cancellationToken) =>
            Task.FromResult(LogDataSourceState.ReadyToOpenReader);
    }
}