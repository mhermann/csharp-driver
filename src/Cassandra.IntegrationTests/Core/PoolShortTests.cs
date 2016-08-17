using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tasks;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class PoolShortTests : TestGlobals
    {
        [TearDown]
        public void OnTearDown()
        {
            TestClusterManager.TryRemove();
        }
        
        [Test]
        public void StopForce_With_Inflight_Requests()
        {
            var testCluster = TestClusterManager.CreateNew(2);
            var builder = Cluster.Builder()
                .AddContactPoint(testCluster.InitialContactPoint)
                .WithPoolingOptions(new PoolingOptions()
                    .SetCoreConnectionsPerHost(HostDistance.Local, 4)
                    .SetMaxConnectionsPerHost(HostDistance.Local, 4))
                .WithLoadBalancingPolicy(new RoundRobinPolicy());
            using (var cluster = builder.Build())
            {
                var session = (Session)cluster.Connect();
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, "ks1", 2));
                session.Execute("CREATE TABLE ks1.table1 (id1 int, id2 int, PRIMARY KEY (id1, id2))");
                var ps = session.Prepare("INSERT INTO ks1.table1 (id1, id2) VALUES (?, ?)");
                Trace.TraceInformation("--Warmup");
                Task.Factory.StartNew(() => ExecuteMultiple(testCluster, session, ps, false, 2).Wait()).Wait();
                Trace.TraceInformation("--Starting");
                Task.Factory.StartNew(() => ExecuteMultiple(testCluster, session, ps, true, 200000).Wait()).Wait();
            }
        }

        private Task<bool> ExecuteMultiple(ITestCluster testCluster, Session session, PreparedStatement ps, bool stopNode, int repeatLength)
        {
            var tcs = new TaskCompletionSource<bool>();
            var semaphore = new AsyncSemaphore(8000);
            var receivedCounter = 0;
            var halfway = repeatLength / 2;
            for (var i = 0; i < repeatLength; i++)
            {
                var index = i;
                semaphore.WaitAsync().ContinueWith(_ =>
                {
                    var statement = ps.Bind(index % 100, index);
                    var t1 = session.ExecuteAsync(statement);
                    t1.ContinueWith(t =>
                    {
                        semaphore.Release();
                        Thread.MemoryBarrier();
                        if (t.Exception != null)
                        {
                            tcs.TrySetException(t.Exception.InnerException);
                            return;
                        }
                        var received = Interlocked.Increment(ref receivedCounter);
                        if (stopNode && received == halfway)
                        {
                            Trace.TraceInformation("Stopping forcefully node2");
                            testCluster.StopForce(2);
                        }
                        if (received == repeatLength)
                        {
                            // Mark this as finished
                            Trace.TraceInformation("--Marking as completed");
                            tcs.TrySetResult(true);
                        }
                    }, TaskContinuationOptions.ExecuteSynchronously);
                });
            }
            return tcs.Task;
        }


        private class AsyncSemaphore
        {
            private readonly Queue<TaskCompletionSource<bool>> _waiters = new Queue<TaskCompletionSource<bool>>();
            private int _currentCount;

            public AsyncSemaphore(int initialCount)
            {
                if (initialCount < 0)
                {
                    throw new ArgumentOutOfRangeException("initialCount");
                }
                _currentCount = initialCount;
            }

            public Task WaitAsync()
            {
                lock (_waiters)
                {
                    if (_currentCount > 0)
                    {
                        --_currentCount;
                        return TaskHelper.Completed;
                    }
                    var w = new TaskCompletionSource<bool>();
                    _waiters.Enqueue(w);
                    return w.Task;
                }
            }

            public void Release()
            {
                TaskCompletionSource<bool> toRelease = null;
                lock (_waiters)
                {
                    if (_waiters.Count > 0)
                    {
                        toRelease = _waiters.Dequeue();
                    }
                    else
                    {
                        ++_currentCount;
                    }
                }
                if (toRelease != null)
                {
                    toRelease.SetResult(true);
                }
            }
        }
    }
}
