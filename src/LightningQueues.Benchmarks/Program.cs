// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Running;
using LightningQueues.Benchmarks;

BenchmarkRunner.Run<SendAndReceive>(new CustomConfig());