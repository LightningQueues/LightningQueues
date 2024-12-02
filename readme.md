LightningQueues - Fast persistent queues for .NET
=====================================================
![.NET (Win, Mac, Linux)](https://github.com/LightningQueues/LightningQueues/workflows/.NET%20Tests/badge.svg)

A fast store and forward message queue for .NET

Why not just use MSMQ?
- 0 Administration required
- XCopy deployable
- XPlat supported

API is very good performance and provides durable persistence sending & receiving messages leveraging
LMDB key/value store via LightningDB.

## How to compile
`dotnet build`

### Run the tests
`dotnet test`

#### Transport Security
There is an example test that shows the hooks available to use TLS encryption
for the stream. The decision is left to the end user on what level of cert 
validation to perform.
