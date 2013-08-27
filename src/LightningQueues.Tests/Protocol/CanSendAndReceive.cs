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
            var wait = new ManualResetEvent(false);

            Message[] recievedMsgs = null;
            var endPoint = new Endpoint("localhost", 23456);
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
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


                wait.WaitOne();


                1.ShouldEqual(recievedMsgs.Length);
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                new byte[] { 1, 2, 4, 5, 6 }.ShouldEqual(recievedMsgs[0].Data);
                new DateTime(2001, 1, 1).ShouldEqual(recievedMsgs[0].SentAt);
            }
        }

        [Test]
        public void TwoMessagesInSeparateCalls()
        {
            var wait = new ManualResetEvent(false);

            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
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


                wait.WaitOne();

                1.ShouldEqual(recievedMsgs.Length);

                wait.Reset();

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

                wait.WaitOne();

                1.ShouldEqual(recievedMsgs.Length);
                "hello doggy2".ShouldEqual(recievedMsgs[0].Queue);
            }
        }

        [Test]
        public void TwoMessagesInOneCall()
        {
            var wait = new ManualResetEvent(false);

            Message[] recievedMsgs = null;
            using (var reciever = new Receiver(new IPEndPoint(IPAddress.Loopback, 23456), messages =>
            {
                recievedMsgs = messages;
                return MockRepository.GenerateStub<IMessageAcceptance>();
            }, ObjectMother.Logger()))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
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


                wait.WaitOne();

                2.ShouldEqual(recievedMsgs.Length);
                "hello doggy".ShouldEqual(recievedMsgs[0].Queue);
                "hello doggy2".ShouldEqual(recievedMsgs[1].Queue);
            }
        }
    }
}