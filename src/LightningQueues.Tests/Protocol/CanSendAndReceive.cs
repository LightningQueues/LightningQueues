using System;
using System.Net;
using System.Threading;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;
using Rhino.Mocks;

namespace LightningQueues.Tests.Protocol
{
    public class CanSendAndReceive 
    {
        [Test]
        public void OneMessage()
        {
            Message[] recievedMsgs = null;
            var endPoint = new Endpoint("localhost", 23456);
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.Start();

                new Sender(ObjectMother.Logger())
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

                recievedMsgs.ShouldHaveCount(1);
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                new byte[] { 1, 2, 4, 5, 6 }.ShouldEqual(recievedMsgs[0].Data);
                new DateTime(2001, 1, 1).ShouldEqual(recievedMsgs[0].SentAt);
            }
        }

        [Test]
        public void TwoMessagesInSeparateCalls()
        {
            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.Start();

                new Sender(ObjectMother.Logger())
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
                recievedMsgs.ShouldHaveCount(1);

                recievedMsgs = null;

                new Sender(ObjectMother.Logger())
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

                recievedMsgs.ShouldHaveCount(1);
                "hello doggy2".ShouldEqual(recievedMsgs[0].Queue);
            }
        }

        [Test]
        public void TwoMessagesInOneCall()
        {
            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.Start();

                new Sender(ObjectMother.Logger())
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

                recievedMsgs.ShouldHaveCount(2);
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                "hello doggy2".ShouldEqual(recievedMsgs[1].Queue);
            }
        }
    }
}