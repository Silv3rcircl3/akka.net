//-----------------------------------------------------------------------
// <copyright file="ActorGraphInterpreter.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using Akka.Actor;
using Akka.Annotations;
using Akka.Event;
using Akka.Pattern;
using Akka.Streams.Stage;
using Akka.Util;
using Reactive.Streams;
using static Akka.Streams.Implementation.Fusing.GraphInterpreter;

// ReSharper disable MemberHidesStaticFromOuterClass
namespace Akka.Streams.Implementation.Fusing
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    [InternalApi]
    public class ActorGraphInterpreter : ActorBase
    {
        public sealed class ResumeActor : IDeadLetterSuppression, INoSerializationVerificationNeeded
        {
            public static ResumeActor Instance {get;} = new ResumeActor();

            private ResumeActor() { }          
        }

        /// <summary>
        /// TBD
        /// </summary>
        public interface IBoundaryEvent : INoSerializationVerificationNeeded, IDeadLetterSuppression
        {
            /// <summary>
            /// TBD
            /// </summary>
            GraphInterpreterShell Shell { get; }

            int Execute(int eventLimit);
        }

        public abstract class SimpleBoundaryEvent : IBoundaryEvent
        {
            public abstract GraphInterpreterShell Shell { get; }

            public int Execute(int eventLimit)
            {
                var wasNotShutDown = !Shell.Interpreter.IsStageCompleted(Logic);
                Execute();

                if(wasNotShutDown)
                    Shell.Interpreter.AfterStageHasRun(Logic);
                return Shell.RunBatch(eventLimit);
            }

            public abstract GraphStageLogic Logic {get;}

            public abstract void Execute();
        }

        /// <summary>
        /// TBD
        /// </summary>
        public class BatchingActorInputBoundary<T> : UpstreamBoundaryStageLogic, IOutHandler, IActorInputBoundary
        {
            #region Messages

            private sealed class OnErrorEvent : SimpleBoundaryEvent
            {
                private readonly BatchingActorInputBoundary<T> _boundary;
                private readonly Exception _cause;

                public OnErrorEvent(BatchingActorInputBoundary<T> boundary, Exception cause)
                {
                    _boundary = boundary;
                    _cause = cause;
                    Shell = boundary._shell;
                    Logic = boundary;
                }

                public override GraphInterpreterShell Shell { get; }

                public override GraphStageLogic Logic { get; }

                public override void Execute()
                {
#pragma warning disable 162
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (IsDebug)
                        // ReSharper disable once HeuristicUnreachableCode
                        Console.WriteLine($"{Shell.Interpreter.Name} OnError port={_boundary._internalPortName}");
#pragma warning restore 162

                    _boundary.OnError(_cause);
                }
            }

            private sealed class OnCompletedEvent : SimpleBoundaryEvent
            {
                private readonly BatchingActorInputBoundary<T> _boundary;

                public OnCompletedEvent(BatchingActorInputBoundary<T> boundary)
                {
                    _boundary = boundary;
                    Shell = boundary._shell;
                    Logic = boundary;
                }

                public override GraphInterpreterShell Shell { get; }

                public override GraphStageLogic Logic { get; }

                public override void Execute()
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (IsDebug)
#pragma warning disable 162
                        // ReSharper disable once HeuristicUnreachableCode
                        Console.WriteLine($"{Shell.Interpreter.Name} OnCompleted port={_boundary._internalPortName}");
#pragma warning restore 162

                    _boundary.OnComplete();
                }
            }

            private sealed class OnNextEvent : SimpleBoundaryEvent
            {
                private readonly BatchingActorInputBoundary<T> _boundary;
                private readonly T _element;

                public OnNextEvent(BatchingActorInputBoundary<T> boundary, T element)
                {
                    _boundary = boundary;
                    _element = element;
                    Shell = boundary._shell;
                    Logic = boundary;
                }

                public override GraphInterpreterShell Shell { get; }

                public override GraphStageLogic Logic { get; }

                public override void Execute()
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (IsDebug)
                        // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                        Console.WriteLine($"{Shell.Interpreter.Name} OnNext port={_boundary._internalPortName}");
#pragma warning restore 162

                    _boundary.OnNext(_element);
                }
            }

            private sealed class OnSubscribeEvent : SimpleBoundaryEvent
            {
                private readonly BatchingActorInputBoundary<T> _boundary;
                private readonly ISubscription _subscription;


                public OnSubscribeEvent(BatchingActorInputBoundary<T> boundary, ISubscription subscription)
                {
                    _boundary = boundary;
                    _subscription = subscription;
                    Shell = boundary._shell;
                    Logic = boundary;
                }

                public override GraphInterpreterShell Shell { get; }

                public override GraphStageLogic Logic { get; }

                public override void Execute()
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (IsDebug)
                        // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                        Console.WriteLine($"{Shell.Interpreter.Name} OnSubscribe port={_boundary._internalPortName}");
#pragma warning restore 162

                    Shell.SubscribeArrived();
                    _boundary.OnSubscribe(_subscription);
                }
            }

            #endregion

            private sealed class Subscription : ISubscriber<T>
            {
                private readonly BatchingActorInputBoundary<T> _boundary;

                public Subscription(BatchingActorInputBoundary<T> boundary) => _boundary = boundary;

                public void OnSubscribe(ISubscription subscription)
                {
                    ReactiveStreamsCompliance.RequireNonNullSubscription(subscription);
                    _boundary.Actor.Tell(new OnSubscribeEvent(_boundary, subscription));
                }

                public void OnNext(T element)
                {
                    ReactiveStreamsCompliance.RequireNonNullElement(element);
                    _boundary.Actor.Tell(new OnNextEvent(_boundary, element));
                }

                public void OnError(Exception cause)
                {
                    ReactiveStreamsCompliance.RequireNonNullException(cause);
                    _boundary.Actor.Tell(new OnErrorEvent(_boundary, cause));
                }

                public void OnComplete() => _boundary.Actor.Tell(new OnCompletedEvent(_boundary));
            }

            private readonly int _size;

            private readonly T[] _inputBuffer;
            private readonly int _indexMask;
            private readonly Outlet<T> _outlet;
            private readonly GraphInterpreterShell _shell;
            private readonly IPublisher<T> _publisher;
            private readonly string _internalPortName;

            private ISubscription _upstream;
            private int _inputBufferElements;
            private int _nextInputElementCursor;
            private bool _upstreamCompleted;
            private bool _downstreamCanceled;
            private int _batchRemaining;
            private readonly int _requestBatchSize;

            public BatchingActorInputBoundary(int size, GraphInterpreterShell shell, IPublisher<T> publisher, string internalPortName)
            {
                if (size <= 0) throw new ArgumentException("buffer size cannot be zero", nameof(size));
                if ((size & (size - 1)) != 0) throw new ArgumentException("buffer size must be a power of two", nameof(size));

                _size = size;  
                _shell = shell;
                _publisher = publisher;
                _internalPortName = internalPortName;

                _indexMask = size - 1;
                _inputBuffer = new T[size];
                _requestBatchSize = Math.Max(1, size / 2);
                _batchRemaining = _requestBatchSize;

                _outlet = new Outlet<T>("UpstreamBoundary: " + internalPortName);
                SetHandler(_outlet, this);
            }
            
            /// <summary>
            /// TBD
            /// </summary>
            public override Outlet Out => _outlet;

            public IActorRef Actor { get; set; } = ActorRefs.NoSender;

            public override void PreStart() => _publisher.Subscribe(new Subscription(this));
            
            public void OnPull()
            {
                var elementsCount = _inputBufferElements;
                var upstreamCompleted = _upstreamCompleted;
                if (elementsCount > 1) Push(_outlet, Dequeue());
                else if (elementsCount == 1)
                {
                    if (upstreamCompleted)
                    {
                        Push(_outlet, Dequeue());
                        Complete(_outlet);
                    }
                    else Push(_outlet, Dequeue());
                }
                else if (upstreamCompleted) Complete(_outlet);
            }

            public void OnDownstreamFinish() => Cancel();

            // Call this when an error happens that does not come from the usual onError channel
            // (exceptions while calling RS interfaces, abrupt termination etc)
            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="reason">TBD</param>
            public void OnInternalError(Exception reason)
            {
                if (!(_upstreamCompleted || _downstreamCanceled))
                    _upstream?.Cancel();

                if (!IsClosed(_outlet))
                    OnError(reason);
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="reason">TBD</param>
            public void OnError(Exception reason)
            {
                if (!_upstreamCompleted || !_downstreamCanceled)
                {
                    _upstreamCompleted = true;
                    Clear();
                    Fail(_outlet, reason);
                }
            }

            /// <summary>
            /// TBD
            /// </summary>
            public void OnComplete()
            {
                if (!_upstreamCompleted)
                {
                    _upstreamCompleted = true;
                    if (_inputBufferElements == 0)
                        Complete(_outlet);
                }
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="subscription">TBD</param>
            /// <exception cref="ArgumentException">TBD</exception>
            public void OnSubscribe(ISubscription subscription)
            {
                if (subscription == null) throw new ArgumentException("Subscription cannot be null");
                if (_upstreamCompleted) ReactiveStreamsCompliance.TryCancel(subscription);
                else if (_downstreamCanceled)
                {
                    _upstreamCompleted = true;
                    ReactiveStreamsCompliance.TryCancel(subscription);
                }
                else
                {
                    _upstream = subscription;
                    // prefetch
                    ReactiveStreamsCompliance.TryRequest(_upstream, _inputBuffer.Length);
                }
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="element">TBD</param>
            /// <exception cref="IllegalStateException">TBD</exception>
            public void OnNext(T element)
            {
                if (!_upstreamCompleted)
                {
                    if (_inputBufferElements == _size)
                        throw new IllegalStateException("Input buffer overrun");
                    _inputBuffer[(_nextInputElementCursor + _inputBufferElements) & _indexMask] = element;
                    _inputBufferElements++;
                    if (IsAvailable(_outlet))
                        Push(_outlet, Dequeue());
                }
            }

            /// <summary>
            /// TBD
            /// </summary>
            public void Cancel()
            {
                _downstreamCanceled = true;
                if (!_upstreamCompleted)
                {
                    _upstreamCompleted = true;
                    if (!ReferenceEquals(_upstream, null))
                        ReactiveStreamsCompliance.TryCancel(_upstream);
                    Clear();
                }
            }

            private T Dequeue()
            {
                var element = _inputBuffer[_nextInputElementCursor];
                if (element == null)
                    throw new IllegalStateException("Internal queue must never contain a null");
                _inputBuffer[_nextInputElementCursor] = default(T);

                _batchRemaining--;
                if (_batchRemaining == 0 && !_upstreamCompleted)
                {
                    ReactiveStreamsCompliance.TryRequest(_upstream, _requestBatchSize);
                    _batchRemaining = _requestBatchSize;
                }

                _inputBufferElements--;
                _nextInputElementCursor = (_nextInputElementCursor + 1) & _indexMask;
                return element;
            }

            private void Clear()
            {
                _inputBuffer.Initialize();
                _inputBufferElements = 0;
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <returns>TBD</returns>
            public override string ToString() => $"BatchingActorInputBoundary(forPort={_internalPortName}, fill={_inputBufferElements}/{_size}, completed={_upstreamCompleted}, canceled={_downstreamCanceled})";
           
        }

        private sealed class SubscribePending<T> : SimpleBoundaryEvent
        {
            private readonly ActorOutputBoundary<T> _boundary;

            public SubscribePending(ActorOutputBoundary<T> boundary)
            {
                _boundary = boundary;
                Shell = boundary.Shell;
                Logic = boundary;
            }

            public override GraphInterpreterShell Shell { get; }
            public override GraphStageLogic Logic { get; }
            public override void Execute() => _boundary.SubscribePending();
        }

        private sealed class RequestMore<T> : SimpleBoundaryEvent
        {
            private readonly ActorOutputBoundary<T> _boundary;
            private readonly long _demand;

            public RequestMore(ActorOutputBoundary<T> boundary, long demand)
            {
                _boundary = boundary;
                _demand = demand;
                Shell = boundary.Shell;
                Logic = boundary;
            }

            public override GraphInterpreterShell Shell { get; }
            public override GraphStageLogic Logic { get; }
            public override void Execute()
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if(IsDebug)
                    // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                    Console.WriteLine($"{_boundary.Shell.Interpreter.Name} request {_demand} port={_boundary.InternalPortName}");
#pragma warning restore 162

                _boundary.RequestMore(_demand);
            }
        }

        private sealed class Cancel<T> : SimpleBoundaryEvent
        {
            private readonly ActorOutputBoundary<T> _boundary;

            public Cancel(ActorOutputBoundary<T> boundary)
            {
                _boundary = boundary;
                Shell = boundary.Shell;
                Logic = boundary;
            }

            public override GraphInterpreterShell Shell { get; }
            public override GraphStageLogic Logic { get; }
            public override void Execute()
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (IsDebug)
                    // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                    Console.WriteLine($"{_boundary.Shell.Interpreter.Name} cancel port={_boundary.InternalPortName}");
#pragma warning restore 162

                _boundary.Cancel();
            }
        }

        private sealed class OutputBoundaryPublisher<T> : IPublisher<T>
        {
            private readonly ActorOutputBoundary<T> _boundary;
            private readonly string _internalPortName;
            private readonly AtomicReference<ImmutableList<ISubscriber<T>>> _pendingSubscribers = new AtomicReference<ImmutableList<ISubscriber<T>>>();
            private Exception _shutdownReason;

            public OutputBoundaryPublisher(ActorOutputBoundary<T> boundary, string internalPortName)
            {
                _boundary = boundary;
                _internalPortName = internalPortName;
                WakeUpMessage = new SubscribePending<T>(boundary);
            }

            private SubscribePending<T> WakeUpMessage { get; }

            public void Subscribe(ISubscriber<T> subscriber)
            {
                ReactiveStreamsCompliance.RequireNonNullSubscriber(subscriber);


                while (true)
                {
                    var current = _pendingSubscribers.Value;

                    if (current == null)
                        ReportSubscribeFailure(subscriber);
                    else
                    {
                        if (_pendingSubscribers.CompareAndSet(current, current.Add(subscriber)))
                            _boundary.StageActor?.Ref.Tell(WakeUpMessage);
                        else
                            continue;
                    }

                    break;
                }
            }

            public ImmutableList<ISubscriber<T>> TakePendingSubscribers()
            {
                var pending = _pendingSubscribers.GetAndSet(null);
                return pending == null ? ImmutableList<ISubscriber<T>>.Empty : pending.Reverse();
            }

            public void Shutdown(Exception reason)
            {
                _shutdownReason = reason;

                var pending = _pendingSubscribers.GetAndSet(null);
                if (pending != null)
                {
                    foreach (var subscriber in pending)
                        ReportSubscribeFailure(subscriber);
                }
                else
                {
                    // already canceled earlier
                }
            }

            private void ReportSubscribeFailure(ISubscriber<T> subscriber)
            {
                try
                {
                    switch (_shutdownReason)
                    {
                        case ISpecViolation _:
                            // ok, not allowed to call OnError
                            break;
                        case Exception e:
                            ReactiveStreamsCompliance.TryOnSubscribe(subscriber, CancelledSubscription.Instance);
                            ReactiveStreamsCompliance.TryOnError(subscriber, e);
                            break;
                        case null:
                            ReactiveStreamsCompliance.TryOnSubscribe(subscriber, CancelledSubscription.Instance);
                            ReactiveStreamsCompliance.TryOnComplete(subscriber);
                            break;
                    }
                }
                catch (Exception e) 
                {
                    if (e is ISpecViolation)
                    {
                        // nothing to do
                    }
                    else
                        throw;
                }
            }

            public override string ToString() => $"Pubsliher[{_internalPortName}]";
        }



        #region internal classes


        /// <summary>
        /// TBD
        /// </summary>
        internal interface IActorOutputBoundary
        {
            IActorRef Actor { get; set; }

            void SubscribePending();

            void RequestMore(long elements);

            void Cancel();

            void Fail(Exception reason);
        }

        internal interface IActorInputBoundary
        {
            IActorRef Actor { get; set; }

            void OnInternalError(Exception exception);
            void Cancel();
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <typeparam name="T">TBD</typeparam>
        internal class ActorOutputBoundary<T> : DownstreamBoundaryStageLogic, IActorOutputBoundary
        {
            #region InHandler
            private sealed class InHandler : Stage.InHandler
            {
                private readonly ActorOutputBoundary<T> _that;

                public InHandler(ActorOutputBoundary<T> that) => _that = that;

                public override void OnPush()
                {
                    _that.OnNext(_that.Grab(_that._inlet));
                    if (_that._downstreamCompleted)
                        _that.Cancel(_that._inlet);
                    else if (_that._downstreamDemand > 0)
                        _that.Pull(_that._inlet);
                }

                public override void OnUpstreamFinish() => _that.Complete();

                public override void OnUpstreamFailure(Exception e) => _that.Fail(e);

                public override string ToString() => _that.ToString();
            }
            #endregion

            internal readonly GraphInterpreterShell Shell;
            internal readonly string InternalPortName;

            private ISubscriber<T> _subscriber;
            private long _downstreamDemand;

            // This flag is only used if complete/fail is called externally since this op turns into a Finished one inside the
            // interpreter (i.e. inside this op this flag has no effects since if it is completed the op will not be invoked)
            private bool _downstreamCompleted;
            private bool _upstreamCompleted;
            private readonly Inlet<T> _inlet;
            private readonly OutputBoundaryPublisher<T> _publisher;

            public ActorOutputBoundary( GraphInterpreterShell shell, string internalPortName)
            {
                Shell = shell;
                InternalPortName = internalPortName;
                _publisher = new OutputBoundaryPublisher<T>(this, internalPortName);
                Publisher = _publisher;
                _inlet = new Inlet<T>("UpstreamBoundary: " + internalPortName) { Id = 0 };
                SetHandler(_inlet, new InHandler(this));
            }

            /// <summary>
            /// TBD
            /// </summary>
            public override Inlet In => _inlet;

            public IPublisher<T> Publisher { get; }

            public IActorRef Actor { get; set; }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="elements">TBD</param>
            public void RequestMore(long elements)
            {
                if (elements < 1)
                {
                    Cancel(_inlet);
                    Fail(ReactiveStreamsCompliance.NumberOfElementsInRequestMustBePositiveException);
                }
                else
                {
                    _downstreamDemand += elements;
                    if (_downstreamDemand < 0)
                        _downstreamDemand = long.MaxValue; // Long overflow, Reactive Streams Spec 3:17: effectively unbounded
                    if (!HasBeenPulled(_inlet) && !IsClosed(_inlet))
                        Pull(_inlet);
                }
            }

            /// <summary>
            /// TBD
            /// </summary>
            public void SubscribePending()
            {
                foreach (var subscriber in _publisher.TakePendingSubscribers())
                {
                    if (ReferenceEquals(_subscriber, null))
                    {
                        _subscriber = subscriber;
                        var subscription = new Subscription(this);

                        ReactiveStreamsCompliance.TryOnSubscribe(_subscriber, subscription);
                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                        if (IsDebug)
                            // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                            Console.WriteLine($"{Interpreter.Name} Subscribe subscriber={subscriber}");
#pragma warning restore 162
                    }
                    else ReactiveStreamsCompliance.RejectAdditionalSubscriber(subscriber, GetType().FullName);
                }
            }

            private sealed class Subscription : ISubscription
            {
                private readonly ActorOutputBoundary<T> _boundary;

                public Subscription(ActorOutputBoundary<T> boundary) => _boundary = boundary;

                public void Cancel() => _boundary.Actor.Tell(new Cancel<T>(_boundary));

                public void Request(long n) => _boundary.Actor.Tell(new RequestMore<T>(_boundary, n));

                public override string ToString() => $"BoundarySubscription[{_boundary.Actor}, {_boundary.InternalPortName}]";
            }

            /// <summary>
            /// TBD
            /// </summary>
            public void Cancel()
            {
                _downstreamCompleted = true;
                _subscriber = null;
                _publisher.Shutdown(new NormalShutdownException("UpstreamBoundary"));
                Cancel(_inlet);
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="reason">TBD</param>
            public void Fail(Exception reason)
            {
                // No need to fail if had already been cancelled, or we closed earlier
                if (!(_downstreamCompleted || _upstreamCompleted))
                {
                    _upstreamCompleted = true;
                    _publisher.Shutdown(reason);
                    if (!ReferenceEquals(_subscriber, null) && !(reason is ISpecViolation))
                        ReactiveStreamsCompliance.TryOnError(_subscriber, reason);
                }
            }

            private void OnNext(T element)
            {
                _downstreamDemand--;
                ReactiveStreamsCompliance.TryOnNext(_subscriber, element);
            }

            private void Complete()
            {
                // No need to complete if had already been cancelled, or we closed earlier
                if (!(_upstreamCompleted || _downstreamCompleted))
                {
                    _upstreamCompleted = true;
                    _publisher.Shutdown(null);
                    if (!ReferenceEquals(_subscriber, null))
                        ReactiveStreamsCompliance.TryOnComplete(_subscriber);
                }
            }
        }

        #endregion
        /// <summary>
        /// INTERNAL API
        /// </summary>
        [InternalApi]
        public sealed class GraphInterpreterShell
        {
            private sealed class AsyncInput : IBoundaryEvent
            {
                private readonly GraphStageLogic _logic;
                private readonly object _e;
                private readonly Action<object> _handler;

                public AsyncInput(GraphInterpreterShell shell, GraphStageLogic logic, object e, Action<object> handler)
                {
                    _logic = logic;
                    _e = e;
                    _handler = handler;
                    Shell = shell;
                }

                public GraphInterpreterShell Shell { get; }

                public int Execute(int eventLimit)
                {
                    if (!Shell._waitingForShutdown)
                    {
                        Shell.Interpreter.RunAsyncInput(_logic, _e, _handler);
                        if (eventLimit == 1 && Shell.Interpreter.IsSuspended)
                        {
                            Shell.SendResume(true);
                            return 0;
                        }

                        return Shell.RunBatch(eventLimit - 1);
                    }

                    return eventLimit;
                }
            }

            internal sealed class ResumeShell : IBoundaryEvent
            {
                public ResumeShell(GraphInterpreterShell shell) => Shell = shell;

                public GraphInterpreterShell Shell { get; }

                public int Execute(int eventLimit)
                {
                    if (!Shell._waitingForShutdown)
                    {
                        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                        if(IsDebug)
                            // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                            Console.WriteLine($"{Shell.Interpreter.Name} resume");
#pragma warning restore 162

                        if (Shell.Interpreter.IsSuspended)
                            return Shell.RunBatch(eventLimit);
                        return eventLimit;
                    }
                    
                    return eventLimit;
                }
            }

            private sealed class Abort : IBoundaryEvent
            {
                public Abort(GraphInterpreterShell shell) => Shell = shell;

                public GraphInterpreterShell Shell { get; }

                public int Execute(int eventLimit)
                {
                    if (Shell._waitingForShutdown)
                    {
                        Shell._subscribersPending = 0;
                        Shell.TryAbort(new TimeoutException("Streaming actor has been already stopped processing (normally), but not all of its " +
                                                            $"inputs or outputs have been subscribed in {Shell._settings.SubscriptionTimeoutSettings.Timeout}. Aborting actor now."));
                    }

                    return 0;
                }
            }

            private readonly Connection[] _connections;
            private readonly GraphStageLogic[] _logics;
            private readonly ActorMaterializerSettings _settings;
            /// <summary>
            /// TBD
            /// </summary>
            internal readonly ExtendedActorMaterializer Materializer;

            /// <summary>
            /// Limits the number of events processed by the interpreter before scheduling
            /// a self-message for fairness with other actors. The basic assumption here is
            /// to give each input buffer slot a chance to run through the whole pipeline
            /// and back (for the elements).
            /// 
            /// Considered use case:
            ///  - assume a composite Sink of one expand and one fold 
            ///  - assume an infinitely fast source of data
            ///  - assume maxInputBufferSize == 1
            ///  - if the event limit is greater than maxInputBufferSize * (ins + outs) than there will always be expand activity
            ///  because no data can enter "fast enough" from the outside
            /// </summary>
            private readonly int _shellEventLimit;

            // Limits the number of events processed by the interpreter on an abort event.
            private readonly int _abortLimit;
            private readonly List<IActorInputBoundary> _inputs = new List<IActorInputBoundary>();
            private readonly List<IActorOutputBoundary> _outputs = new List<IActorOutputBoundary>();

            private ILoggingAdapter _log;
            private GraphInterpreter _interpreter;
            private int _subscribersPending;
            private bool _resumeScheduled;
            private bool _waitingForShutdown;
            private Action<object> _enqueueToShortCircuit;
            private bool _interpreterCompleted;
            private readonly ResumeShell _resume;

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="connections">TBD</param>
            /// <param name="logics">TBD</param>
            /// <param name="settings">TbD</param>
            /// <param name="materializer">TBD</param>
            public GraphInterpreterShell(Connection[] connections, GraphStageLogic[] logics, ActorMaterializerSettings settings, ExtendedActorMaterializer materializer)
            {
                _connections = connections;
                _logics = logics;
                _settings = settings;
                Materializer = materializer;
                
                _shellEventLimit = settings.MaxInputBufferSize * 16;
                _abortLimit = _shellEventLimit * 2;

                _resume = new ResumeShell(this);
            }

            /// <summary>
            /// TBD
            /// </summary>
            public bool IsInitialized => Self != null;
            /// <summary>
            /// TBD
            /// </summary>
            public bool IsTerminated => _interpreterCompleted && CanShutdown;
            /// <summary>
            /// TBD
            /// </summary>
            public bool CanShutdown => _subscribersPending == 0;
            /// <summary>
            /// TBD
            /// </summary>
            public IActorRef Self { get; private set; }
            /// <summary>
            /// TBD
            /// </summary>
            public ILoggingAdapter Log => _log ?? (_log = GetLogger());
            /// <summary>
            /// TBD
            /// </summary>
            public GraphInterpreter Interpreter => _interpreter ?? (_interpreter = GetInterpreter());

            public Connection[] Connections { get; set; }
            public GraphStageLogic[] Logics { get; set; }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="self">TBD</param>
            /// <param name="subMat">TBD</param>
            /// <param name="enqueueToShourtCircuit">TBD</param>
            /// <param name="eventLimit">TBD</param>
            /// <returns>TBD</returns>
            public int Init(IActorRef self, SubFusingActorMaterializerImpl subMat, Action<object> enqueueToShourtCircuit, int eventLimit)
            {
                Self = self;
                _enqueueToShortCircuit = enqueueToShourtCircuit;

                foreach (var logic in _logics)
                {
                    switch (logic)
                    {
                        case IActorInputBoundary input:
                            input.Actor = Self;
                            _subscribersPending++;
                            _inputs.Add(input);
                            break;
                        case IActorOutputBoundary output:
                            output.Actor = Self;
                            output.SubscribePending();
                            _outputs.Add(output);
                            break;
                    }
                }
                
                Interpreter.Init(subMat);
                return RunBatch(eventLimit);
            }

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="e">TBD</param>
            /// <param name="eventLimit">TBD</param>
            /// <returns>TBD</returns>
            public int ProcessEvents(IBoundaryEvent e, int eventLimit)
            {
                _resumeScheduled = false;
                return e.Execute(eventLimit);
            }

            /**
             * Attempts to abort execution, by first propagating the reason given until either
             *  - the interpreter successfully finishes
             *  - the event limit is reached
             *  - a new error is encountered
             */
            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="reason">TBD</param>
            /// <exception cref="IllegalStateException">TBD</exception>
            /// <returns>TBD</returns>
            public void TryAbort(Exception reason)
            {
                var ex = reason is ISpecViolation
                    ? new IllegalStateException("Shutting down because of violation of the Reactive Streams specification",
                        reason)
                    : reason;

                // This should handle termination while interpreter is running. If the upstream have been closed already this
                // call has no effect and therefore does the right thing: nothing.
                try
                {
                    foreach (var input in _inputs)
                        input.OnInternalError(ex);

                    Interpreter.Execute(_abortLimit);
                    Interpreter.Finish();
                }
                catch (Exception) { /* swallow? */ }
                finally
                {
                    _interpreterCompleted = true;
                    // Will only have an effect if the above call to the interpreter failed to emit a proper failure to the downstream
                    // otherwise this will have no effect
                    foreach (var output in _outputs)
                        output.Fail(ex);
                    foreach (var input in _inputs)
                        input.Cancel();
                }
            }

            internal int RunBatch(int actorEventLimit)
            {
                try
                {
                    var usingShellLimit = _shellEventLimit < actorEventLimit;
                    var remainingQuota = _interpreter.Execute(Math.Min(actorEventLimit, _shellEventLimit));

                    if (Interpreter.IsCompleted)
                    {
                        // Cannot stop right away if not completely subscribed
                        if (CanShutdown)
                            _interpreterCompleted = true;
                        else
                        {
                            _waitingForShutdown = true;
                            Materializer.ScheduleOnce(_settings.SubscriptionTimeoutSettings.Timeout,
                                () => Self.Tell(new Abort(this)));
                        }
                    }
                    else if (Interpreter.IsSuspended && !_resumeScheduled)
                        SendResume(!usingShellLimit);

                    return usingShellLimit ? actorEventLimit - _shellEventLimit + remainingQuota : remainingQuota;
                }
                catch (Exception reason)
                {
                    TryAbort(reason);
                    return actorEventLimit - 1;
                }
            }

            private void SendResume(bool sendResume)
            {
                _resumeScheduled = true;
                if (sendResume)
                    Self.Tell(_resume);
                else
                    _enqueueToShortCircuit(_resume);
            }

            private GraphInterpreter GetInterpreter()
            {
                return new GraphInterpreter(Materializer, Log, _logics, _connections,
                    (logic, @event, handler) =>
                    {
                        var asyncInput = new AsyncInput(this, logic, @event, handler);
                        var currentInterpreter = CurrentInterpreterOrNull;
                        if (currentInterpreter == null || !Equals(currentInterpreter.Context, Self))
                            Self.Tell(new AsyncInput(this, logic, @event, handler));
                        else
                            _enqueueToShortCircuit(asyncInput);
                    }, _settings.IsFuzzingMode, Self);
            }

            private BusLogging GetLogger()
            {
                return new BusLogging(Materializer.System.EventStream, Self.ToString(), typeof(GraphInterpreterShell), new DefaultLogMessageFormatter());
            }

            public void SubscribeArrived() => _subscribersPending--;

            public override string ToString() => "GraphInterpreterShell\n";
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="shell">TBD</param>
        /// <returns>TBD</returns>
        public static Props Props(GraphInterpreterShell shell) => Actor.Props.Create(() => new ActorGraphInterpreter(shell)).WithDeploy(Deploy.Local);

        private ISet<GraphInterpreterShell> _activeInterpreters = new HashSet<GraphInterpreterShell>();
        private readonly Queue<GraphInterpreterShell> _newShells = new Queue<GraphInterpreterShell>();
        private readonly SubFusingActorMaterializerImpl _subFusingMaterializerImpl;
        private readonly GraphInterpreterShell _initial;
        private ILoggingAdapter _log;
        //this limits number of messages that can be processed synchronously during one actor receive.
        private readonly int _eventLimit;
        private int _currentLimit;
        //this is a var in order to save the allocation when no short-circuiting actually happens
        private Queue<object> _shortCircuitBuffer;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="shell">TBD</param>
        public ActorGraphInterpreter(GraphInterpreterShell shell)
        {
            _initial = shell;

            _subFusingMaterializerImpl = new SubFusingActorMaterializerImpl(shell.Materializer, RegisterShell);
            _eventLimit = _initial.Materializer.Settings.SyncProcessingLimit;
            _currentLimit = _eventLimit;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public ILoggingAdapter Log => _log ?? (_log = Context.GetLogger());

        private void EnqueueToShortCircuit(object input)
        {
            if(_shortCircuitBuffer == null)
                _shortCircuitBuffer = new Queue<object>();

            _shortCircuitBuffer.Enqueue(input);
        }

        private bool TryInit(GraphInterpreterShell shell)
        {
            try
            {
                _currentLimit = shell.Init(Self, _subFusingMaterializerImpl, EnqueueToShortCircuit, _currentLimit);
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (IsDebug)
                    // ReSharper disable once HeuristicUnreachableCode
#pragma warning disable 162
                    Console.WriteLine($"registering new shell in {_initial}\n  {shell.ToString().Replace("\n", "\n  ")}");
#pragma warning restore 162
                if (shell.IsTerminated)
                    return false;
                _activeInterpreters.Add(shell);
                return true;
            }
            catch (Exception e)
            {
                if (Log.IsErrorEnabled)
                    Log.Error(e, "Initialization of GraphInterpreterShell failed for {0}", shell);
                return false;
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="shell">TBD</param>
        /// <returns>TBD</returns>
        public IActorRef RegisterShell(GraphInterpreterShell shell)
        {
            _newShells.Enqueue(shell);
            EnqueueToShortCircuit(new GraphInterpreterShell.ResumeShell(shell));
            return Self;
        }

        // Avoid performing the initialization (which starts the first RunBatch())
        // within RegisterShell in order to avoid unbounded recursion.
        private void FinishShellRegistration()
        {
            if (_newShells.Count == 0)
            {
                if (_activeInterpreters.Count == 0)
                    Context.Stop(Self);
            }
            else
            {
                var shell = _newShells.Dequeue();
                if (shell.IsInitialized)
                {
                    // yes, this steals another shell's Resume, but that's okay because extra ones will just not do anything
                    FinishShellRegistration();
                }
                else if (!TryInit(shell))
                {
                    if (_activeInterpreters.Count == 0)
                        FinishShellRegistration();
                }
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        protected override void PreStart()
        {
            TryInit(_initial);
            if (_activeInterpreters.Count == 0)
                Context.Stop(Self);
            else if (_shortCircuitBuffer != null)
                ShortCircuitBatch();
        }

        private void ShortCircuitBatch()
        {
            while (_shortCircuitBuffer.Count != 0 && _currentLimit > 0 && _activeInterpreters.Count != 0)
            {
                var element = _shortCircuitBuffer.Dequeue();
                if (element is IBoundaryEvent boundary)
                    ProcessEvent(boundary);
                else if (element is ResumeActor)
                    FinishShellRegistration();
            }

            if(_shortCircuitBuffer.Count != 0 && _currentLimit == 0)
                Self.Tell(ResumeActor.Instance);
        }

        private void ProcessEvent(IBoundaryEvent b)
        {
            var shell = b.Shell;
            if (!shell.IsTerminated && (shell.IsInitialized || TryInit(shell)))
            {
                try
                {
                    _currentLimit = shell.ProcessEvents(b, _currentLimit);
                }
                catch (Exception ex)
                {
                    shell.TryAbort(ex);
                }

                if (shell.IsTerminated)
                {
                    _activeInterpreters.Remove(shell);
                    if(_activeInterpreters.Count == 0 && _newShells.Count == 0)
                        Context.Stop(Self);
                }
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="message">TBD</param>
        /// <returns>TBD</returns>
        protected override bool Receive(object message)
        {
            switch (message)
            {
                case IBoundaryEvent _:
                    _currentLimit = _eventLimit;
                    ProcessEvent((IBoundaryEvent)message);
                    if (_shortCircuitBuffer != null)
                        ShortCircuitBatch();
                    return true;
                case ResumeActor _:
                    _currentLimit = _eventLimit;
                    if (_shortCircuitBuffer != null)
                        ShortCircuitBatch();
                    return true;
                case StreamSupervisor.PrintDebugDump _:
                    var builder = new StringBuilder($"activeShells (actor: {Self}):\n");

                    foreach (var shell in _activeInterpreters)
                    {
                        builder.Append("  " + shell.ToString().Replace("\n", "\n  "));
                        builder.Append(shell.Interpreter);
                    }

                    builder.AppendLine("NewShells:\n");

                    foreach (var shell in _newShells)
                    {
                        builder.Append("  " + shell.ToString().Replace("\n", "\n  "));
                        builder.Append(shell.Interpreter);
                    }

                    Console.WriteLine(builder);
                    return true;
                default: return false;
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        protected override void PostStop()
        {
            var ex = new AbruptTerminationException(Self);
            foreach (var shell in _activeInterpreters)
                shell.TryAbort(ex);
            _activeInterpreters = new HashSet<GraphInterpreterShell>();
            foreach (var shell in _newShells)
            {
                if (TryInit(shell))
                    shell.TryAbort(ex);
            }
        }
    }
}