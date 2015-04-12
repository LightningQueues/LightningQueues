using System;
using System.Net;
using Should;
using LightningQueues.Model;
using LightningQueues.Protocol;
using Xunit;
using Rhino.Mocks;

namespace LightningQueues.Tests.Protocol
{
    public class CanSendAndReceive 
    {
        [Fact(Skip="Not on mono")]
        public void OneMessage()
        {
            Message[] recievedMsgs = null;
            var endPoint = new Endpoint("localhost", 23456);
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }))
            {
                reciever.Start();

                new Sender()
                {
                    Destination = endPoint,
                    Messages = new[]
                    {
                        new Message
                        {
                            Data = new byte[] {1, 2, 4, 5, 6},
                            SentAt = new DateTime(2001, 1, 1),
                            Queue = "hello doggy",
                            Id = MessageId.GenerateRandom()
                        },
                    }
                }.Send();


                Wait.Until(() => recievedMsgs != null).ShouldBeTrue();

                recievedMsgs.ShouldBeEmpty();
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                new byte[] { 1, 2, 4, 5, 6 }.ShouldEqual(recievedMsgs[0].Data);
                new DateTime(2001, 1, 1).ShouldEqual(recievedMsgs[0].SentAt);
            }
        }

        [Fact(Skip="Not on mono")]
        public void TwoMessagesInSeparateCalls()
        {
            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }))
            {
                reciever.Start();

                new Sender()
                {
                    Destination = new Endpoint("localhost", 23456),
                    Messages = new[]
                    {
                        new Message
                        {
                            Data = new byte[] {1, 2, 4, 5, 6},
                            SentAt = new DateTime(2001, 1, 1),
                            Queue = "hello doggy",
                            Id = MessageId.GenerateRandom()
                        },
                    }
                }.Send();

                Wait.Until(() => recievedMsgs != null).ShouldBeTrue();
                recievedMsgs.ShouldBeEmpty();

                recievedMsgs = null;

                new Sender()
                {
                    Destination = new Endpoint("localhost", 23456),
                    Messages = new[]
                    {
                        new Message
                        {
                            Data = new byte[] {1, 2, 4, 5, 6},
                            SentAt = new DateTime(2001, 1, 1),
                            Queue = "hello doggy2",
                            Id = MessageId.GenerateRandom()
                        },
                    }
                }.Send();

                Wait.Until(() => recievedMsgs != null).ShouldBeTrue();

                recievedMsgs.ShouldBeEmpty();
                "hello doggy2".ShouldEqual(recievedMsgs[0].Queue);
            }
        }

        [Fact(Skip="Not on mono")]
        public void TwoMessagesInOneCall()
        {
            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }))
            {
                reciever.Start();

                new Sender()
                {
                    Destination = new Endpoint("localhost", 23456),
                    Messages = new[]
                    {
                        new Message
                        {
                            Data = new byte[] {1, 2, 4, 5, 6},
                            SentAt = new DateTime(2001, 1, 1),
                            Queue = "hello doggy",
                            Id = MessageId.GenerateRandom()
                        },
                         new Message
                        {
                            Data = new byte[] {1, 2, 4, 5, 6},
                            SentAt = new DateTime(2001, 1, 1),
                            Queue = "hello doggy2",
                            Id = MessageId.GenerateRandom()
                        },
                    }
                }.Send();

                Wait.Until(() => recievedMsgs != null).ShouldBeTrue();

                recievedMsgs.Length.ShouldEqual(2);
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                "hello doggy2".ShouldEqual(recievedMsgs[1].Queue);
            }
        }
    }
}