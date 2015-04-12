using Xunit;
using Should;
using System;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Protocol.Reactive;
using static System.Text.Encoding;

namespace LightningQueues.Tests.Protocol.Reactive
{
    public class ProtocolTests
    {
        private readonly ReactiveSendingProtocol _sender;
        
        public ProtocolTests()
        {
            _sender = new ReactiveSendingProtocol();
        }

        [Fact]
        public void sending_with_default_protocol_single_message()
        {
            var sendMessage = SimpleMessage("hello reactive");
            using(var stream = new MemoryStream())
            {

                var streams = (from _ in _sender.WriteLength(stream, sendMessage.Item2.Length)
                               from m in _sender.WriteMessages(stream, sendMessage.Item2, sendMessage.Item1)
                               select stream).Do(x => x.Position = 0);
                var receiver = new ReactiveReceivingProtocol();
                var readyForReceive = receiver.ReceiveStream(streams);
                readyForReceive.ObserveOn(Scheduler.CurrentThread)
                    .Subscribe(x => UTF8.GetString(x.Data).ShouldEqual("hello reactive"));
            }
        }

        private Tuple<Message[], byte[]> SimpleMessage(string text)
        {
            var message = new Message { Queue = "test", Data = UTF8.GetBytes(text), Id = MessageId.GenerateRandom()};
            var messages = new [] { message };
            return new Tuple<Message[], byte[]>(messages, messages.Serialize());
        }

    }
}
