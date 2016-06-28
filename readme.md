LightningQueues - Fast persistent queues for .NET
=====================================================

[![Join the chat at https://gitter.im/LightningQueues/LightningQueues](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/LightningQueues/LightningQueues?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Linux Build Status](https://travis-ci.org/LightningQueues/LightningQueues.svg?branch=dotnet)](https://travis-ci.org/LightningQueues/LightningQueues)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/xquoegvd1qriv8wy/branch/dotnet?svg=true)](https://ci.appveyor.com/project/CoreyKaylor/lightningqueues/branch/dotnet)

A fast store and forward message queue for .NET. (aka not a broker or server)

Why not just use MSMQ?
- 0 Administration required
- XCopy deployable
- XPlat supported

API is completely rewritten using reactive extensions from top to bottom. 
Everything is completely asynchronous and provides at-least-once delivery for your messages.