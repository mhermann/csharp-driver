//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a pool of connections to a host
    /// </summary>
    internal class HostConnectionPool : IDisposable
    {
        private const int ConnectionIndexOverflow = int.MaxValue - 100000;
        private readonly static Logger Logger = new Logger(typeof(HostConnectionPool));
        private readonly static Connection[] EmptyConnectionsArray = new Connection[0];
        //Safe iteration of connections
        private readonly CopyOnWriteList<Connection> _connections = new CopyOnWriteList<Connection>();
        private readonly Host _host;
        private readonly HostDistance _distance;
        private readonly Configuration _config;
        private readonly Serializer _serializer;
        private readonly HashedWheelTimer _timer;
        private int _connectionIndex;
        private HashedWheelTimer.ITimeout _timeout;
        private volatile bool _isShuttingDown;
        private int _isIncreasingSize;
        private TaskCompletionSource<Connection[]> _creationTcs;
        private volatile bool _isDisposed;

        /// <summary>
        /// Gets a list of connections already opened to the host
        /// </summary>
        public IEnumerable<Connection> OpenConnections 
        { 
            get { return _connections; }
        }

        public HostConnectionPool(Host host, HostDistance distance, Configuration config, Serializer serializer)
        {
            _host = host;
            _host.CheckedAsDown += OnHostCheckedAsDown;
            _host.Down += OnHostDown;
            _host.Up += OnHostUp;
            _host.Remove += OnHostRemoved;
            _distance = distance;
            _config = config;
            _serializer = serializer;
            _timer = config.Timer;
        }

        /// <summary>
        /// Gets an open connection from the host pool (creating if necessary).
        /// It returns null if the load balancing policy didn't allow connections to this host.
        /// </summary>
        public Task<Connection> BorrowConnection()
        {
            return MaybeCreateFirstConnection().ContinueSync(poolConnections =>
            {
                if (poolConnections.Length == 0)
                {
                    //The load balancing policy stated no connections for this host
                    return null;
                }
                var connection = MinInFlight(poolConnections, ref _connectionIndex);
                MaybeIncreasePoolSize(connection.InFlight);
                return connection;
            });
        }

        /// <summary>
        /// Gets the connection with the minimum number of InFlight requests.
        /// Only checks for index + 1 and index, to avoid a loop of all connections.
        /// </summary>
        public static Connection MinInFlight(Connection[] connections, ref int connectionIndex)
        {
            if (connections.Length == 1)
            {
                return connections[0];
            }
            //It is very likely that the amount of InFlight requests per connection is the same
            //Do round robin between connections, skipping connections that have more in flight requests
            var index = Interlocked.Increment(ref connectionIndex);
            if (index > ConnectionIndexOverflow)
            {
                //Overflow protection, not exactly thread-safe but we can live with it
                Interlocked.Exchange(ref connectionIndex, 0);
            }
            var currentConnection = connections[index % connections.Length];
            var previousConnection = connections[(index - 1)%connections.Length];
            if (previousConnection.InFlight < currentConnection.InFlight)
            {
                return previousConnection;
            }
            return currentConnection;
        }

        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        internal virtual Task<Connection> CreateConnection()
        {
            Logger.Info("Creating a new connection to the host " + _host.Address);
            var c = new Connection(_serializer, _host.Address, _config);
            return c.Open().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    if (_config.GetPoolingOptions(_serializer.ProtocolVersion).GetHeartBeatInterval() > 0)
                    {
                        //Heartbeat is enabled, subscribe for possible exceptions
                        c.OnIdleRequestException += OnIdleRequestException;
                    }
                    return c;
                }
                Logger.Info("The connection to {0} could not be opened", _host.Address);
                c.Dispose();
                if (t.Exception != null)
                {
                    t.Exception.Handle(_ => true);
                    Logger.Error(t.Exception.InnerException);
                    throw t.Exception.InnerException;
                }
                throw new TaskCanceledException("The connection creation task was cancelled");
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Handler that gets invoked when if there is a socket exception when making a heartbeat/idle request
        /// </summary>
        private void OnIdleRequestException(Exception ex)
        {
            _host.SetDown();
        }

        internal void OnHostCheckedAsDown(Host h, long delay)
        {
            if (!_host.SetAttemptingReconnection())
            {
                //Another pool is attempting reconnection
                //Eventually Host.Up event is going to be fired.
                return;
            }
            //Schedule next reconnection attempt (without using the timer thread)
            //Cancel the previous one
            var nextTimeout = _timer.NewTimeout(_ => Task.Factory.StartNew(AttemptReconnection), null, delay);
            SetReconnectionTimeout(nextTimeout);
        }

        /// <summary>
        /// Handles the reconnection attempts.
        /// If it succeeds, it marks the host as UP.
        /// If not, it marks the host as DOWN
        /// </summary>
        internal void AttemptReconnection()
        {
            _isShuttingDown = false;
            if (_isDisposed)
            {
                return;
            }
            var tcs = new TaskCompletionSource<Connection[]>();
            //While there is a single thread here, there might be another thread
            //Calling MaybeCreateFirstConnection()
            //Guard for multiple creations
            var creationTcs = Interlocked.CompareExchange(ref _creationTcs, tcs, null);
            if (creationTcs != null || _connections.Count > 0)
            {
                //Already creating as host is back UP (possibly via events)
                return;
            }
            Logger.Info("Attempting reconnection to host {0}", _host.Address);
            //There is a single thread creating a connection
            CreateConnection().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    if (_isShuttingDown)
                    {
                        t.Result.Dispose();
                        TransitionCreationTask(tcs, EmptyConnectionsArray);
                        return;
                    }
                    _connections.Add(t.Result);
                    Logger.Info("Reconnection attempt to host {0} succeeded", _host.Address);
                    _host.BringUpIfDown();
                    TransitionCreationTask(tcs, new [] { t.Result });
                    return;
                }
                Logger.Info("Reconnection attempt to host {0} failed", _host.Address);
                Exception ex = null;
                if (t.Exception != null)
                {
                    t.Exception.Handle(e => true);
                    ex = t.Exception.InnerException;
                    //This makes sure that the exception is observed, but still sets _creationTcs' exception
                    //for MaybeCreateFirstConnection
                    tcs.Task.ContinueWith(x =>
                    {
                        if (x.Exception != null)
                            x.Exception.Handle(_ => true);
                    });
                }
                TransitionCreationTask(tcs, EmptyConnectionsArray, ex);
                _host.SetDown(failedReconnection: true);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void OnHostUp(Host host)
        {
            _isShuttingDown = false;
            SetReconnectionTimeout(null);
            //The host is back up, we can start creating the pool (if applies)
            MaybeCreateFirstConnection();
        }

        private void OnHostDown(Host h, long delay)
        {
            Shutdown();
        }

        /// <summary>
        /// Cancels the previous and set the next reconnection timeout, as an atomic operation.
        /// </summary>
        private void SetReconnectionTimeout(HashedWheelTimer.ITimeout nextTimeout)
        {
            var timeout = Interlocked.Exchange(ref _timeout, nextTimeout);
            if (timeout != null)
            {
                timeout.Cancel();
            }
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty.
        /// It may return an empty array if its being closed.
        /// It may return an array of connections being closed.
        /// </summary>
        internal Task<Connection[]> MaybeCreateFirstConnection()
        {
            var tcs = new TaskCompletionSource<Connection[]>();
            var connections = _connections.GetSnapshot();
            if (connections.Length > 0)
            {
                tcs.SetResult(connections);
                return tcs.Task;
            }
            var creationTcs = Interlocked.CompareExchange(ref _creationTcs, tcs, null);
            if (creationTcs != null)
            {
                return creationTcs.Task;
            }
            //Could have transitioned
            connections = _connections.GetSnapshot();
            if (connections.Length > 0)
            {
                TransitionCreationTask(tcs, connections);
                return tcs.Task;
            }
            if (_isShuttingDown)
            {
                //It transitioned to DOWN, avoid try to create new Connections
                TransitionCreationTask(tcs, EmptyConnectionsArray);
                return tcs.Task;
            }
            Logger.Info("Initializing pool to {0}", _host.Address);
            //There is a single thread creating a single connection
            CreateConnection().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    if (_isShuttingDown)
                    {
                        //Is shutting down
                        t.Result.Dispose();
                        TransitionCreationTask(tcs, EmptyConnectionsArray);
                        return;
                    }
                    _connections.Add(t.Result);
                    _host.BringUpIfDown();
                    TransitionCreationTask(tcs, new[] { t.Result });
                    return;
                }
                if (t.Exception != null)
                {
                    TransitionCreationTask(tcs, null, t.Exception.InnerException);
                    return;
                }
                TransitionCreationTask(tcs, EmptyConnectionsArray);
            }, TaskContinuationOptions.ExecuteSynchronously);
            return tcs.Task;
        }

        private void TransitionCreationTask(TaskCompletionSource<Connection[]> tcs, Connection[] result, Exception ex = null)
        {
            if (ex != null)
            {
                tcs.TrySetException(ex);
            }
            else if (result != null)
            {
                tcs.TrySetResult(result);
            }
            else
            {
                tcs.TrySetException(new DriverInternalError("Creation task must transition from a result or an exception"));
            }
            Interlocked.Exchange(ref _creationTcs, null);
        }

        /// <summary>
        /// Increases the size of the pool from 1 to core and from core to max
        /// </summary>
        /// <returns>True if it is creating a new connection</returns>
        internal bool MaybeIncreasePoolSize(int inFlight)
        {
            var protocolVersion = _serializer.ProtocolVersion;
            var coreConnections = _config.GetPoolingOptions(protocolVersion).GetCoreConnectionsPerHost(_distance);
            var connections = _connections.GetSnapshot();
            if (connections.Length == 0)
            {
                return false;
            }
            if (connections.Length >= coreConnections)
            {
                var maxInFlight = _config.GetPoolingOptions(protocolVersion).GetMaxSimultaneousRequestsPerConnectionTreshold(_distance);
                var maxConnections = _config.GetPoolingOptions(protocolVersion).GetMaxConnectionPerHost(_distance);
                if (inFlight < maxInFlight)
                {
                    return false;
                }
                if (_connections.Count >= maxConnections)
                {
                    return false;
                }
            }
            var isAlreadyIncreasing = Interlocked.CompareExchange(ref _isIncreasingSize, 1, 0) == 1;
            if (isAlreadyIncreasing)
            {
                return true;
            }
            if (_isShuttingDown || _connections.Count == 0)
            {
                Interlocked.Exchange(ref _isIncreasingSize, 0);
                return false;
            }
            CreateConnection().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    if (_isShuttingDown)
                    {
                        //Is shutting down
                        t.Result.Dispose();
                    }
                    else
                    {
                        _connections.Add(t.Result);   
                    }
                }
                if (t.Exception != null)
                {
                    Logger.Error("Error while increasing pool size", t.Exception.InnerException);
                }
                Interlocked.Exchange(ref _isIncreasingSize, 0);
            }, TaskContinuationOptions.ExecuteSynchronously);
            return true;
        }

        public void CheckHealth(Connection c)
        {
            if (c.TimedOutOperations < _config.SocketOptions.DefunctReadTimeoutThreshold)
            {
                return;
            }
            //We are in the default thread-pool (non-io thread)
            //Defunct: close it and remove it from the pool
            _connections.Remove(c);
            c.Dispose();
        }

        public void Shutdown()
        {
            _isShuttingDown = true;
            var connections = _connections.ClearAndGet();
            if (connections.Length == 0)
            {
                return;
            }
            Logger.Info(string.Format("Shutting down pool to {0}, closing {1} connection(s).", _host.Address, connections.Length));
            foreach (var c in connections)
            {
                c.Dispose();
            }
        }

        private void OnHostRemoved()
        {
            Dispose();
        }

        /// <summary>
        /// Releases the resources associated with the pool.
        /// </summary>
        public void Dispose()
        {
            _isDisposed = true;
            SetReconnectionTimeout(null);
            Shutdown();
            _host.CheckedAsDown -= OnHostCheckedAsDown;
            _host.Up -= OnHostUp;
            _host.Down -= OnHostDown;
            _host.Remove -= OnHostRemoved;
        }
    }

    internal class HostConnectionPool2 : IDisposable
    {
        private static readonly Logger Logger = new Logger(typeof(HostConnectionPool));
        private const int ConnectionIndexOverflow = int.MaxValue - 100000;

        /// <summary>
        /// Represents the possible states of the pool.
        /// Possible state transitions:
        ///  - From Init to Closing: The pool must be closed because the host is ignored or because the pool should
        ///    not attempt more reconnections (another pool is trying to reconnect to a UP host).
        ///  - From Init to ShuttingDown: The pool is being shutdown as a result of a client shutdown.
        ///  - From Closing to Init: The pool finished closing connections (is now ignored) and it resets to
        ///    initial state in case the host is marked as local/remote in the future.
        ///    // ^ TODO, how to control it? Host.Up event?
        ///  - From Closing to ShuttingDown (rare): It was marked as ignored, now the client is being shutdown.
        ///  - From ShuttingDown to Shutdown: Finished shutting down, the pool should not be reused.
        /// </summary>
        private static class PoolState
        {
            /// <summary>
            /// Initial state: open / opening / ready to be opened
            /// </summary>
            public const int Init = 0;
            /// <summary>
            /// When the pool is being closed as part of a distance change
            /// </summary>
            public const int Closing = 1;
            /// <summary>
            /// When the pool is being shutdown for good
            /// </summary>
            public const int ShuttingDown = 2;
            /// <summary>
            /// When the pool has being shutdown
            /// </summary>
            public const int Shutdown = 3;
        }

        private readonly Host _host;
        private readonly Configuration _config;
        private readonly Serializer _serializer;
        private readonly CopyOnWriteList<Connection> _connections = new CopyOnWriteList<Connection>();
        private readonly HashedWheelTimer _timer;
        private volatile IReconnectionSchedule _reconnectionSchedule;
        private volatile int _coreConnectionLength;
        //TODO: private volatile int _maxConnectionLength;
        private int _state = PoolState.Init;
        private HashedWheelTimer.ITimeout _newConnectionTimeout;
        private TaskCompletionSource<Connection> _connectionOpenTcs;
        private int _connectionIndex;

        public event Action<HostConnectionPool2> AllConnectionClosed;

        public bool HasConnections
        {
            get { return _connections.Count > 0; }
        }

        public int OpenConnections
        {
            get { return _connections.Count; }
        }

        public bool IsClosing
        {
            get { return Volatile.Read(ref _state) != PoolState.Init; }
        }

        public HostConnectionPool2(Host host, Configuration config, Serializer serializer)
        {
            _host = host;
            _host.Down += OnHostDown;
            _host.Up += OnHostUp;
            _host.Remove += OnHostRemoved;
            _config = config;
            _serializer = serializer;
            _timer = config.Timer;
            _reconnectionSchedule = config.Policies.ReconnectionPolicy.NewSchedule();
            _coreConnectionLength = 1;
        }

        /// <summary>
        /// Releases the resources associated with the pool.
        /// </summary>
        public void Dispose()
        {
            // Mark as shuttingDown (once?)
            //TODO:  Shutdown();
            //TODO: close pool
            _host.Up -= OnHostUp;
            _host.Down -= OnHostDown;
            _host.Remove -= OnHostRemoved;
        }

        private void OnConnectionClosing(Connection c = null)
        {
            if (c != null)
            {
                _connections.Remove(c);
            }
            if (IsClosing || _connections.Count >= _coreConnectionLength)
            {
                // No need to reconnect
                return;
            }
            if (_connections.Count == 0)
            {
                if (AllConnectionClosed != null)
                {
                    AllConnectionClosed(this);
                }
                return;
            }
            SetNewConnectionTimeout(_reconnectionSchedule);
        }

        private void OnHostRemoved()
        {
            //TODO: Drain and shutdown
            throw new NotImplementedException();
        }

        public void OnHostUp(Host h)
        {
            Logger.Info("Pool #{0} for host {1} attempting to reconnect as host is UP", GetHashCode(), _host.Address);
            ScheduleReconnection(true);
        }

        private void OnHostDown(Host h, long delay)
        {
            //TODO: Cancel any reconnection attempt
            throw new NotImplementedException();
        }

        /// <summary>
        /// Handler that gets invoked when if there is a socket exception when making a heartbeat/idle request
        /// </summary>
        private void OnIdleRequestException(Exception ex)
        {
            //TODO: Remove connection from pool and dispose it
        }

        /// <summary>
        /// Sets the state of the pool to Closing
        /// </summary>
        public void StartClosing()
        {
            var isClosing = Interlocked.CompareExchange(ref _state, PoolState.Closing, PoolState.Init) == PoolState.Init;
            if (!isClosing)
            {
                // It was in another state, don't mind
                return;
            }
            var previousTimeout = Interlocked.Exchange(ref _newConnectionTimeout, null);
            if (previousTimeout != null)
            {
                // Clear previous reconnection attempt timeout
                previousTimeout.Cancel();
            }
        }

        /// <summary>
        /// Adds a new reconnection timeout using a new schedule.
        /// Resets the status of the pool to allow further reconnections.
        /// </summary>
        public void ScheduleReconnection(bool immediate = false)
        {
            var schedule = _config.Policies.ReconnectionPolicy.NewSchedule();
            _reconnectionSchedule = schedule;
            Interlocked.Exchange(ref _state, PoolState.Init);
            SetNewConnectionTimeout(immediate ? null : schedule);
        }

        private void SetNewConnectionTimeout(IReconnectionSchedule schedule)
        {
            if (schedule != null && _reconnectionSchedule != schedule)
            {
                // There's another reconnection schedule, leave it
                return;
            }
            HashedWheelTimer.ITimeout timeout = null;
            if (schedule != null)
            {
                // Schedule the creation
                timeout = _timer.NewTimeout(_ => StartCreatingConnection(schedule), null, schedule.NextDelayMs());
            }
            else
            {
                // Start creating immediately on another thread
                Task.Run(() => StartCreatingConnection(null));
            }
            var previousTimeout = Interlocked.Exchange(ref _newConnectionTimeout, timeout);
            if (previousTimeout != null)
            {
                // Clear previous reconnection attempt timeout
                previousTimeout.Cancel();
            }
        }

        /// <summary>
        /// Asynchronously starts to create a new connection (if its not already being created).
        /// A <c>null</c> schedule signals that the pool is not reconnecting but growing to the expected size.
        /// </summary>
        /// <param name="schedule"></param>
        private void StartCreatingConnection(IReconnectionSchedule schedule)
        {
            if (_connections.Count >= _coreConnectionLength)
            {
                return;
            }
            CreateOpenConnection().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    //TODO: Emit `ConnectionCreated`
                    StartCreatingConnection(null);
                    return;
                }
                // The connection could not be opened
                if (IsClosing)
                {
                    // don't mind, the pool is not supposed to being open
                    return;
                }
                if (schedule == null)
                {
                    // As it failed, we need a new schedule for the following attempts
                    schedule = _config.Policies.ReconnectionPolicy.NewSchedule();
                    _reconnectionSchedule = schedule;
                }
                if (schedule != _reconnectionSchedule)
                {
                    // There's another reconnection schedule, leave it
                    return;
                }
                OnConnectionClosing();
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        public virtual async Task<Connection> DoCreateAndOpen()
        {
            var c = new Connection(_serializer, _host.Address, _config);
            try
            {
                await c.Open().ConfigureAwait(false);
            }
            catch
            {
                c.Dispose();
                throw;
            }
            if (_config.GetPoolingOptions(_serializer.ProtocolVersion).GetHeartBeatInterval() > 0)
            {
                c.OnIdleRequestException += OnIdleRequestException;
            }
            c.Closing += OnConnectionClosing;
            return c;
        }

        /// <summary>
        /// Opens one connection. 
        /// If a connection is being opened it yields the same task, preventing creation in parallel.
        /// </summary>
        /// <exception cref="System.Net.Sockets.SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        public async Task<Connection> CreateOpenConnection()
        {
            var tcs = new TaskCompletionSource<Connection>();
            var concurrentOpenTcs = Interlocked.CompareExchange(ref _connectionOpenTcs, tcs, null);
            if (concurrentOpenTcs != null)
            {
                // There is another thread opening a new connection
                return await concurrentOpenTcs.Task;
            }
            if (IsClosing)
            {
                throw GetNotConnectedException();
            }
            Logger.Info("Creating a new connection to {0}", _host.Address);
            Connection c;
            try
            {
                c = await DoCreateAndOpen().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.Info("Connection to {0} could not be created: {1}", _host.Address, ex);
                tcs.TrySetException(ex);
                // Clean the internal state to allow other threads to create a connection
                Interlocked.Exchange(ref _connectionOpenTcs, null);
                throw;
            }
            if (IsClosing)
            {
                Logger.Info("Connection to {0} opened successfully but pool #{1} was being closed", _host.Address);
                var ex = GetNotConnectedException();
                Interlocked.Exchange(ref _connectionOpenTcs, null);
                c.Dispose();
                tcs.TrySetException(ex);
                throw ex;
            }
            var newLength = _connections.AddNew(c);
            Logger.Info("Connection to {0} opened successfully, pool #{1} length: {2}", _host.Address, GetHashCode(), newLength);
            tcs.TrySetResult(c);
            Interlocked.Exchange(ref _connectionOpenTcs, null);
            return c;
        }

        private static SocketException GetNotConnectedException()
        {
            return new SocketException((int)SocketError.NotConnected);
        }

        public async Task<Connection[]> EnsureCreate()
        {
            var connections = _connections.GetSnapshot();
            if (connections.Length > 0)
            {
                return connections;
            }
            try
            {
                var c = await CreateOpenConnection();
                StartCreatingConnection(null);
                return new[] {c};
            }
            catch (Exception)
            {
                OnConnectionClosing();
                throw;
            }
        }

        public void SetDistance(HostDistance distance)
        {
            _coreConnectionLength = _config.GetPoolingOptions(_serializer.ProtocolVersion).GetCoreConnectionsPerHost(distance);
            //TODO: _maxConnectionLength = _config.GetPoolingOptions(_serializer.ProtocolVersion).GetMaxConnectionPerHost(distance);
        }

        /// <summary>
        /// Gets an open connection from the host pool (creating if necessary).
        /// It returns null if the load balancing policy didn't allow connections to this host.
        /// </summary>
        public async Task<Connection> BorrowConnection()
        {
            var connections = await EnsureCreate();
            if (connections.Length == 0)
            {
                return null;
            }
            return MinInFlight(connections, ref _connectionIndex);
        }

        /// <summary>
        /// Gets the connection with the minimum number of InFlight requests.
        /// Only checks for index + 1 and index, to avoid a loop of all connections.
        /// </summary>
        public static Connection MinInFlight(Connection[] connections, ref int connectionIndex)
        {
            if (connections.Length == 1)
            {
                return connections[0];
            }
            //It is very likely that the amount of InFlight requests per connection is the same
            //Do round robin between connections, skipping connections that have more in flight requests
            var index = Interlocked.Increment(ref connectionIndex);
            if (index > ConnectionIndexOverflow)
            {
                //Overflow protection, not exactly thread-safe but we can live with it
                Interlocked.Exchange(ref connectionIndex, 0);
            }
            var currentConnection = connections[index % connections.Length];
            var previousConnection = connections[(index - 1) % connections.Length];
            if (previousConnection.InFlight < currentConnection.InFlight)
            {
                return previousConnection;
            }
            return currentConnection;
        }
    }
}
