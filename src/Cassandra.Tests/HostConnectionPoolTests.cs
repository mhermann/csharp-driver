#if !NETCORE
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Moq;
using NUnit.Framework;
// ReSharper disable AccessToModifiedClosure

namespace Cassandra.Tests
{
    [TestFixture]
    public class HostConnectionPoolTests
    {
        private static readonly IPEndPoint Address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1000);
        private const byte ProtocolVersion = 2;
        private static readonly Host Host1 = TestHelper.CreateHost("127.0.0.1");
        private static readonly HashedWheelTimer Timer = new HashedWheelTimer();

        [OneTimeSetUp]
        public void OnTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            Trace.Listeners.Add(new ConsoleTraceListener());
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            Timer.Dispose();
        }

        private static IPEndPoint GetIpEndPoint(byte lastByte = 1)
        {
            return new IPEndPoint(IPAddress.Parse("127.0.0." + lastByte), 9042);
        }

        private static Connection CreateConnection(byte lastIpByte = 1, Configuration config = null)
        {
            if (config == null)
            {
                config = GetConfig();
            }
            return new Connection(new Serializer(ProtocolVersion), GetIpEndPoint(lastIpByte), config);
        }

        private static Mock<HostConnectionPool2> GetPoolMock(Host host = null, Configuration config = null)
        {
            if (host == null)
            {
                host = Host1;
            }
            if (config == null)
            {
                config = GetConfig();
            }
            return new Mock<HostConnectionPool2>(host, config, new Serializer(ProtocolVersion));
        }

        private static Configuration GetConfig(int coreConnections = 3, int maxConnections = 8, IReconnectionPolicy rp = null)
        {
            var pooling = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, coreConnections)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 1500)
                .SetMaxConnectionsPerHost(HostDistance.Local, maxConnections);
            var policies = new Policies(
                Policies.DefaultLoadBalancingPolicy,
                rp ?? Policies.DefaultReconnectionPolicy,
                Policies.DefaultRetryPolicy,
                Policies.DefaultSpeculativeExecutionPolicy);
            var config = new Configuration(
                policies,
                new ProtocolOptions(),
                pooling,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            config.Timer = Timer;
            return config;
        }

        private static Connection GetConnectionMock(int inflight, int timedOutOperations = 0)
        {
            var config = new Configuration
            {
                BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager()
            };
            var connectionMock = new Mock<Connection>(MockBehavior.Loose, new Serializer(4), Address, config);
            connectionMock.Setup(c => c.InFlight).Returns(inflight);
            connectionMock.Setup(c => c.TimedOutOperations).Returns(timedOutOperations);
            return connectionMock.Object;
        }

        public HostConnectionPoolTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Info;
        }

        [Test]
        public void MaybeCreateFirstConnection_Should_Yield_The_First_Connection_Opened()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)lastByte++), 200 - lastByte * 50));
            var pool = mock.Object;
            var creation = pool.EnsureCreate();
            creation.Wait();
            Assert.AreEqual(1, creation.Result.Length);
            //yield the third connection first
            CollectionAssert.AreEqual(new[] { GetIpEndPoint() }, creation.Result.Select(c => c.Address));
        }

        [Test]
        public void EnsureCreate_Should_Yield_A_Connection_If_Any_Fails()
        {
            var mock = GetPoolMock();
            var counter = 0;
            //use different addresses for same hosts to differentiate connections: for test only
            //different connections to same hosts should use the same address
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                if (++counter == 2)
                {
                    return TaskHelper.FromException<Connection>(new Exception("Dummy exception"));
                }
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            var creation = pool.EnsureCreate();
            creation.Wait();
            Assert.AreEqual(creation.Result.Length, 1);
        }

        [Test]
        public void EnsureCreate_Serial_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 1;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                var c = CreateConnection((byte)lastByte++);
                if (lastByte == 2)
                {
                    return TestHelper.DelayedTask(c, 500);
                }
                return TaskHelper.ToTask(c);
            });
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[4];
            creationTasks[0] = pool.EnsureCreate();
            creationTasks[1] = pool.EnsureCreate();
            creationTasks[2] = pool.EnsureCreate();
            creationTasks[3] = pool.EnsureCreate();
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[0]).Length);
            for (var i = 1; i < creationTasks.Length; i++)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(creationTasks[i]).Length);
            }
        }

        [Test]
        public void EnsureCreate_Parallel_Calls_Should_Yield_First()
        {
            var mock = GetPoolMock();
            var lastByte = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask(CreateConnection((byte)++lastByte), 100 + (lastByte > 1 ? 10000 : 0)));
            var pool = mock.Object;
            var creationTasks = new Task<Connection[]>[10];
            var counter = -1;
            var initialCreate = pool.EnsureCreate();
            TestHelper.ParallelInvoke(() =>
            {
                creationTasks[Interlocked.Increment(ref counter)] = pool.EnsureCreate();
            }, 10);
            // ReSharper disable once CoVariantArrayConversion
            Task.WaitAll(creationTasks);
            Assert.AreEqual(1, TaskHelper.WaitToComplete(initialCreate).Length);

            foreach (var t in creationTasks)
            {
                Assert.AreEqual(1, TaskHelper.WaitToComplete(t).Length);
            }
            Assert.AreEqual(1, lastByte);
        }

        [Test]
        public void EnsureCreate_Fail_To_Open_All_Connections_Should_Fault_Task()
        {
            var mock = GetPoolMock();
            var testException = new Exception("Dummy exception");
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask<Connection>(() =>
            {
                throw testException;
            }));
            var pool = mock.Object;
            var task = pool.EnsureCreate();
            var ex = Assert.Throws<Exception>(() => TaskHelper.WaitToComplete(task));
            Assert.AreEqual(testException, ex);
        }

        [Test]
        public void OnHostUp_Recreates_Pool_In_The_Background()
        {
            var mock = GetPoolMock(null, GetConfig(2, 2));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 500, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            pool.OnHostUp(null);
            Thread.Sleep(100);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(1, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            Thread.Sleep(500);
            Assert.AreEqual(1, pool.OpenConnections);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            Thread.Sleep(500);
            Assert.AreEqual(2, pool.OpenConnections);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
        }

        [Test]
        public async Task EnsureCreate_After_Reconnection_Attempt_Waits_Existing()
        {
            var mock = GetPoolMock(null, GetConfig(2, 2));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 300, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            Thread.Sleep(100);
            pool.OnHostUp(null);
            await pool.EnsureCreate();
            Assert.AreEqual(1, pool.OpenConnections);
            Thread.Sleep(1000);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(2, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
        }

        [Test]
        public async Task EnsureCreate_Can_Handle_Multiple_Concurrent_Calls()
        {
            var mock = GetPoolMock(null, GetConfig(3, 3));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 50, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            Assert.AreEqual(0, pool.OpenConnections);
            var tasks = new Task[100];
            for (var i = 0; i < 100; i++)
            {
                tasks[i] = Task.Run(async () => await pool.EnsureCreate());
            }
            await Task.WhenAll(tasks);
            Assert.Greater(pool.OpenConnections, 0);
            Assert.LessOrEqual(pool.OpenConnections, 3);
            await Task.Delay(400);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            Assert.AreEqual(3, pool.OpenConnections);
        }

        [Test]
        public async Task EnsureCreate_Should_Increase_One_At_A_Time_Until_Core_Connections()
        {
            var mock = GetPoolMock(null, GetConfig(3, 3));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 500, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            await pool.EnsureCreate();
            Assert.AreEqual(1, pool.OpenConnections);
            Thread.Sleep(100);
            //No connections added yet
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            Thread.Sleep(500);
            Assert.AreEqual(2, pool.OpenConnections);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            Thread.Sleep(500);
            Assert.AreEqual(3, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Thread.Sleep(500);
            Assert.AreEqual(3, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
        }

        [Test]
        public async Task ConsiderResizingPool_Should_Increase_One_At_A_Time_Until_Max_Connections()
        {
            var mock = GetPoolMock(null, GetConfig(1, 3));
            var creationCounter = 0;
            var isCreating = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref creationCounter);
                Interlocked.Exchange(ref isCreating, 1);
                return TestHelper.DelayedTask(CreateConnection(), 50, () => Interlocked.Exchange(ref isCreating, 0));
            });
            var pool = mock.Object;
            pool.SetDistance(HostDistance.Local);
            await pool.EnsureCreate();
            // Low in-flight
            pool.ConsiderResizingPool(20);
            Assert.AreEqual(1, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            // Above threshold
            pool.ConsiderResizingPool(1501);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            // Wait for the creation
            await Task.Delay(100);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            // Wait for the connection pool to allow further connections
            await Task.Delay(2100);
            // Below threshold
            pool.ConsiderResizingPool(20);
            Assert.AreEqual(2, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            // Above threshold
            pool.ConsiderResizingPool(1700);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(1, Volatile.Read(ref isCreating));
            // Wait for the creation
            await Task.Delay(100);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            // Wait for the connection pool to allow further connections
            await Task.Delay(2100);
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
            // Reached max: above threshold
            pool.ConsiderResizingPool(1700);
            // Still max
            Assert.AreEqual(3, Volatile.Read(ref creationCounter));
            Assert.AreEqual(0, Volatile.Read(ref isCreating));
        }

        [Test]
        public void MinInFlight_Returns_The_Min_Inflight_From_Two_Connections()
        {
            var connections = new[]
            {
                GetConnectionMock(0),
                GetConnectionMock(1),
                GetConnectionMock(1),
                GetConnectionMock(10),
                GetConnectionMock(1)
            };
            var index = 1;
            var c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 2);
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 3);
            //previous had less in flight
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 4);
            Assert.AreSame(connections[4], c);
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 5);
            Assert.AreSame(connections[0], c);
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 6);
            Assert.AreSame(connections[0], c);
            index = 9;
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 10);
            Assert.AreSame(connections[0], c);
        }

        [Test]
        public void MinInFlight_Goes_Through_All_The_Connections_When_Over_Threshold()
        {
            var connections = new[]
            {
                GetConnectionMock(10),
                GetConnectionMock(1),
                GetConnectionMock(201),
                GetConnectionMock(200),
                GetConnectionMock(210)
            };
            var index = 1;
            var c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 2);
            Assert.AreSame(connections[1], c);
            c = HostConnectionPool2.MinInFlight(connections, ref index, 100);
            Assert.AreEqual(index, 3);
            // Should pick the first below the threshold
            Assert.AreSame(connections[0], c);
        }

        [Test]
        public void ScheduleReconnection_Should_Reconnect_In_The_Background()
        {
            var mock = GetPoolMock(null, GetConfig(1, 1, new ConstantReconnectionPolicy(50)));
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 20));
            var pool = mock.Object;
            Assert.AreEqual(0, pool.OpenConnections);
            pool.ScheduleReconnection();
            Thread.Sleep(200);
            Assert.AreEqual(1, pool.OpenConnections);
        }

        [Test]
        public void ScheduleReconnection_Should_Raise_AllConnectionClosed()
        {
            var mock = GetPoolMock(null, GetConfig(1, 1, new ConstantReconnectionPolicy(100)));
            var openConnectionsAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                Interlocked.Increment(ref openConnectionsAttempts);
                return TaskHelper.FromException<Connection>(new Exception("Test Exception"));
            });
            var pool = mock.Object;
            var eventRaised = 0;
            pool.AllConnectionClosed += _ => Interlocked.Increment(ref eventRaised);
            pool.ScheduleReconnection();
            Thread.Sleep(600);
            Assert.AreEqual(0, pool.OpenConnections);
            Assert.AreEqual(1, Volatile.Read(ref eventRaised));
            // Should not retry to reconnect, should relay on external consumer
            Assert.AreEqual(1, Volatile.Read(ref openConnectionsAttempts));
        }

        [Test]
        public void ScheduleReconnection_Try_To_Reconnect_While_There_Is_At_Least_One_Connection()
        {
            var mock = GetPoolMock(null, GetConfig(3, 3, new ConstantReconnectionPolicy(100)));
            var openConnectionsAttempts = 0;
            mock.Setup(p => p.DoCreateAndOpen()).Returns(() =>
            {
                var attempts = Interlocked.Increment(ref openConnectionsAttempts);
                if (attempts == 1)
                {
                    // First connection succeeded
                    return TaskHelper.ToTask(CreateConnection());
                }
                if (attempts < 5)
                {
                    // 2 to 4 attempts: fail
                    return TaskHelper.FromException<Connection>(new Exception("Test Exception"));
                }
                return TaskHelper.ToTask(CreateConnection());
            });
            var pool = mock.Object;
            var eventRaised = 0;
            pool.AllConnectionClosed += _ => Interlocked.Increment(ref eventRaised);
            pool.SetDistance(HostDistance.Local);
            pool.ScheduleReconnection();
            Thread.Sleep(300);
            Assert.AreEqual(1, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref eventRaised));
            // Should retry to reconnect
            Assert.Greater(Volatile.Read(ref openConnectionsAttempts), 1);
            Thread.Sleep(1000);
            Assert.AreEqual(3, pool.OpenConnections);
            Assert.AreEqual(0, Volatile.Read(ref eventRaised));
            Assert.AreEqual(6, Volatile.Read(ref openConnectionsAttempts));
        }


        //        [Test]
        //        public void AttemptReconnection_Should_Not_Create_A_New_Connection_If_There_Is_An_Open_Connection()
        //        {
        //            var mock = GetPoolMock();
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
        //            var pool = mock.Object;
        //            //Create 1 connection
        //            TaskHelper.WaitToComplete(pool.MaybeCreateFirstConnection());
        //            Thread.SpinWait(1);
        //            Assert.AreEqual(1, pool.OpenConnections.Count());
        //            pool.AttemptReconnection();
        //            Thread.SpinWait(5);
        //            //Pool remains the same
        //            Assert.AreEqual(1, pool.OpenConnections.Count());
        //        }
        //
        //        [Test]
        //        public void AttemptReconnection_After_Dispose_Should_Not_Create_New_Connections()
        //        {
        //            var mock = GetPoolMock();
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 20));
        //            var pool = mock.Object;
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //            pool.Dispose();
        //            pool.AttemptReconnection();
        //            Thread.Sleep(100);
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //        }
        //
        //        [Test]
        //        public void Dispose_Should_Cancel_Reconnection_Attempts()
        //        {
        //            var mock = GetPoolMock();
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TestHelper.DelayedTask(CreateConnection(), 200));
        //            var pool = mock.Object;
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //            pool.AttemptReconnection();
        //            pool.Dispose();
        //            Thread.Sleep(500);
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //        }
        //
        //        [Test]
        //        public void OnHostCheckedAsDown_Should_Not_Schedule_Reconnection_When_Host_Is_Already_Reconnecting()
        //        {
        //            var host = TestHelper.CreateHost("127.0.0.1");
        //            var config = GetConfig(2);
        //            config.Timer = new HashedWheelTimer(100, 10);
        //            var mock = GetPoolMock(host, config);
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
        //            var pool = mock.Object;
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //            //fake other pool getting the flag
        //            host.SetAttemptingReconnection();
        //            //attempt reconnection should exit
        //            pool.OnHostCheckedAsDown(host, 10);
        //            Thread.Sleep(400);
        //            //Pool remains the same
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //            config.Timer.Dispose();
        //        }
        //
        //        [Test]
        //        public void OnHostCheckedAsDown_Should_Schedule_Reconnection()
        //        {
        //            var host = TestHelper.CreateHost("127.0.0.1");
        //            var config = GetConfig(2);
        //            config.Timer = new HashedWheelTimer(100, 10);
        //            var mock = GetPoolMock(host, config);
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(CreateConnection()));
        //            var pool = mock.Object;
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //            //attempt reconnection should exit
        //            pool.OnHostCheckedAsDown(host, 10);
        //            Thread.Sleep(400);
        //            //Pool should grow
        //            Assert.AreEqual(1, pool.OpenConnections.Count());
        //            config.Timer.Dispose();
        //        }
        //
        //        [Test]
        //        public void CheckHealth_Removes_Connection()
        //        {
        //            var host = TestHelper.CreateHost("127.0.0.1");
        //            var connection1 = GetConnectionMock(100, SocketOptions.DefaultDefunctReadTimeoutThreshold + 10);
        //            var mock = GetPoolMock(host, connection1.Configuration);
        //            mock.Setup(p => p.CreateConnection()).Returns(() => TaskHelper.ToTask(connection1));
        //            var pool = mock.Object;
        //            pool.MaybeCreateFirstConnection().Wait();
        //            Assert.AreEqual(1, pool.OpenConnections.Count());
        //            var c = pool.OpenConnections.FirstOrDefault();
        //            Assert.NotNull(c);
        //            pool.CheckHealth(c);
        //            Assert.AreEqual(0, pool.OpenConnections.Count());
        //        }
    }
}
#endif