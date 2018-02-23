//-----------------------------------------------------------------------
// <copyright file="StreamLayout.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Annotations;
using Akka.Pattern;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Stages;
using Akka.Util;
using Reactive.Streams;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// TBD
    /// </summary>
    public static class StreamLayout
    {
        /// <summary>
        /// TBD
        /// </summary>
        public static readonly bool IsDebug = false;
    }

    /// <summary>
    /// This is the only extension point for the sealed type hierarchy: composition
    /// (i.e. the module tree) is managed strictly within this file, only leaf nodes
    /// may be declared elsewhere.
    /// </summary>
    public interface IAtomicModule<out TShape> : IGraph<TShape> where TShape : Shape
    {
    }

    /// <summary>
    /// INTERNAL API
    /// 
    /// This is a transparent processor that shall consume as little resources as
    /// possible. Due to the possibility of receiving uncoordinated inputs from both
    /// downstream and upstream, this needs an atomic state machine which looks a
    /// little like this:
    /// 
    /// <![CDATA[
    ///            +--------------+      (2)    +------------+
    ///            |     null     | ----------> | Subscriber |
    ///            +--------------+             +------------+
    ///                   |                           |
    ///               (1) |                           | (1)
    ///                  \|/                         \|/
    ///            +--------------+      (2)    +------------+ --\
    ///            | Subscription | ----------> |    Both    |    | (4)
    ///            +--------------+             +------------+ <-/
    ///                   |                           |
    ///               (3) |                           | (3)
    ///                  \|/                         \|/
    ///            +--------------+      (2)    +------------+ --\
    ///            |   Publisher  | ----------> |   Inert    |    | (4, *)
    ///            +--------------+             +------------+ <-/
    /// ]]>
    /// The idea is to keep the major state in only one atomic reference. The actions
    /// that can happen are:
    /// 
    ///  (1) onSubscribe
    ///  (2) subscribe
    ///  (3) onError / onComplete
    ///  (4) onNext
    ///      (*) Inert can be reached also by cancellation after which onNext is still fine
    ///          so we just silently ignore possible spec violations here
    /// 
    /// Any event that occurs in a state where no matching outgoing arrow can be found
    /// is a spec violation, leading to the shutdown of this processor (meaning that
    /// the state is updated such that all following actions match that of a failed
    /// Publisher or a cancelling Subscriber, and the non-guilty party is informed if
    /// already connected).
    /// 
    /// request() can only be called after the Subscriber has received the Subscription
    /// and that also means that onNext() will only happen after having transitioned into
    /// the Both state as well. The Publisher state means that if the real
    /// Publisher terminates before we get the Subscriber, we can just forget about the
    /// real one and keep an already finished one around for the Subscriber.
    /// 
    /// The Subscription that is offered to the Subscriber must cancel the original
    /// Publisher if things go wrong (like `request(0)` coming in from downstream) and
    /// it must ensure that we drop the Subscriber reference when `cancel` is invoked.
    /// </summary>
    [InternalApi]
    public sealed class VirtualProcessor<T> : AtomicReference<object>, IProcessor<T, T>
    {
        private const string NoDemand = "spec violation: OnNext was signaled from upstream without demand";

        #region internal classes

        private sealed class Inert
        {
            public static readonly ISubscriber<T> Subscriber = new CancellingSubscriber<T>();

            public static readonly Inert Instance = new Inert();

            private Inert()
            {
            }
        }

        private sealed class Both
        {
            public Both(ISubscriber<T> subscriber)
            {
                Subscriber = subscriber;
            }

            public ISubscriber<T> Subscriber { get; }

        }

        #endregion

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="subscriber">TBD</param>
        /// <exception cref="Exception">TBD</exception>
        public void Subscribe(ISubscriber<T> subscriber)
        {
            if (subscriber == null)
            {
                var ex = ReactiveStreamsCompliance.SubscriberMustNotBeNullException;
                try
                {
                    TrySubscribe(Inert.Subscriber);
                }
                finally
                {
                    // must throw ArgumentNullEx, rule 2:13
                    throw ex;
                }
            }

            TrySubscribe(subscriber);
        }

        private void TrySubscribe(ISubscriber<T> subscriber)
        {
            if (Value == null)
            {
                if (!CompareAndSet(null, subscriber))
                    TrySubscribe(subscriber);
                return;
            }

            if (Value is ISubscription subscription)
            {
                if (CompareAndSet(subscription, new Both(subscriber)))
                    EstablishSubscription(subscriber, subscription);
                else
                    TrySubscribe(subscriber);

                return;
            }

            if (Value is IPublisher<T> publisher)
            {
                if (CompareAndSet(publisher, Inert.Instance))
                    publisher.Subscribe(subscriber);
                else
                    TrySubscribe(subscriber);

                return;
            }

            ReactiveStreamsCompliance.RejectAdditionalSubscriber(subscriber, "VirtualProcessor");
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="subscription">TBD</param>
        /// <exception cref="Exception">TBD</exception>
        public void OnSubscribe(ISubscription subscription)
        {
            if (subscription == null)
            {
                var ex = ReactiveStreamsCompliance.SubscriptionMustNotBeNullException;
                try
                {
                    TryOnSubscribe(new ErrorPublisher<T>(ex, "failed-VirtualProcessor"), subscription);
                }
                finally
                {
                    // must throw ArgumentNullEx, rule 2:13
                    throw ex;
                }
            }

            TryOnSubscribe(subscription, subscription);
        }

        private void TryOnSubscribe(object obj, ISubscription s)
        {
            if (Value == null)
            {
                if (!CompareAndSet(null, obj))
                    TryOnSubscribe(obj, s);
                return;
            }

            if (Value is ISubscriber<T> subscriber)
            {
                if (obj is ISubscription subscription)
                {
                    if (CompareAndSet(subscriber, new Both(subscriber)))
                        EstablishSubscription(subscriber, subscription);
                    else
                        TryOnSubscribe(obj, s);

                    return;
                }

                if (Value is IPublisher<T> publisher)
                {
                    var inert = GetAndSet(Inert.Instance);
                    if (inert != Inert.Instance)
                        publisher.Subscribe(subscriber);
                    return;
                }

                return;
            }

            // spec violation
            ReactiveStreamsCompliance.TryCancel(s);
        }

        private void EstablishSubscription(ISubscriber<T> subscriber, ISubscription subscription)
        {
            var wrapped = new WrappedSubscription(subscription, this);
            try
            {
                subscriber.OnSubscribe(wrapped);
                // Requests will be only allowed once onSubscribe has returned to avoid reentering on an onNext before
                // onSubscribe completed
                wrapped.UngateDemandAndRequestBuffered();
            }
            catch (Exception ex)
            {
                Value = Inert.Instance;
                ReactiveStreamsCompliance.TryCancel(subscription);
                ReactiveStreamsCompliance.TryOnError(subscriber, ex);
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="cause">TBD</param>
        /// <exception cref="Exception">TBD</exception>
        public void OnError(Exception cause)
        {
            /*
            * `ex` is always a reasonable Throwable that we should communicate downstream,
            * but if `t` was `null` then the spec requires us to throw an NPE (which `ex`
            * will be in this case).
            */
            var ex = cause ?? ReactiveStreamsCompliance.ExceptionMustNotBeNullException;

            while (true)
            {
                if (Value == null)
                {
                    if (!CompareAndSet(null, new ErrorPublisher<T>(ex, "failed-VirtualProcessor")))
                        continue;
                    if (cause == null)
                        throw ex;
                    return;
                }

                if (Value is ISubscription subscription)
                {
                    if (!CompareAndSet(subscription, new ErrorPublisher<T>(ex, "failed-VirtualProcessor")))
                        continue;
                    if (cause == null)
                        throw ex;
                    return;
                }

                if (Value is Both both)
                {
                    Value = Inert.Instance;
                    try
                    {
                        ReactiveStreamsCompliance.TryOnError(both.Subscriber, ex);
                    }
                    finally
                    {
                        // must throw ArgumentNullEx, rule 2:13
                        if (cause == null)
                            throw ex;
                    }

                    return;
                }

                if (Value is ISubscriber<T> subscriber)
                {
                    // spec violation
                    var inert = GetAndSet(Inert.Instance);
                    if (inert != Inert.Instance)
                        new ErrorPublisher<T>(ex, "failed-VirtualProcessor").Subscribe(subscriber);

                    return;
                }

                // spec violation or cancellation race, but nothing we can do
                return;
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        public void OnComplete()
        {
            while (true)
            {
                if (Value == null)
                {
                    if (!CompareAndSet(null, EmptyPublisher<T>.Instance))
                        continue;

                    return;
                }

                if (Value is ISubscription subscription)
                {
                    if (!CompareAndSet(subscription, EmptyPublisher<T>.Instance))
                        continue;

                    return;
                }

                if (Value is Both both)
                {
                    Value = Inert.Instance;
                    ReactiveStreamsCompliance.TryOnComplete(both.Subscriber);
                    return;
                }

                if (Value is ISubscriber<T> subscriber)
                {
                    // spec violation
                    Value = Inert.Instance;
                    EmptyPublisher<T>.Instance.Subscribe(subscriber);
                    return;
                }

                // spec violation or cancellation race, but nothing we can do
                return;
            }
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="element">TBD</param>
        /// <exception cref="Exception">TBD</exception>
        /// <exception cref="IllegalStateException">TBD</exception>
        public void OnNext(T element)
        {
            if (element == null)
            {
                var ex = ReactiveStreamsCompliance.ElementMustNotBeNullException;

                while (true)
                {
                    if (Value == null || Value is ISubscription)
                    {
                        if (!CompareAndSet(Value, new ErrorPublisher<T>(ex, "failed-VirtualProcessor")))
                            continue;

                        break;
                    }

                    if (Value is ISubscriber<T> subscriber)
                    {
                        try
                        {
                            subscriber.OnError(ex);
                        }
                        finally
                        {
                            Value = Inert.Instance;
                        }
                        break;
                    }

                    if (Value is Both both)
                    {
                        try
                        {
                            both.Subscriber.OnError(ex);
                        }
                        finally
                        {
                            Value = Inert.Instance;
                        }
                    }

                    // spec violation or cancellation race, but nothing we can do
                    break;
                }

                // must throw ArgumentNullEx, rule 2:13
                throw ex;
            }

            while (true)
            {
                if (Value is Both both)
                {
                    try
                    {
                        both.Subscriber.OnNext(element);
                        return;
                    }
                    catch (Exception e)
                    {
                        Value = Inert.Instance;
                        throw new IllegalStateException(
                            "Subscriber threw exception, this is in violation of rule 2:13", e);
                    }
                }

                if (Value is ISubscriber<T> subscriber)
                {
                    // spec violation
                    var ex = new IllegalStateException(NoDemand);
                    var inert = GetAndSet(Inert.Instance);
                    if (inert != Inert.Instance)
                        new ErrorPublisher<T>(ex, "failed-VirtualProcessor").Subscribe(subscriber);
                    throw ex;
                }

                if (Value == Inert.Instance || Value is IPublisher<T>)
                {
                    // nothing to be done
                    return;
                }

                var publisher = new ErrorPublisher<T>(new IllegalStateException(NoDemand), "failed-VirtualPublisher");
                if (!CompareAndSet(Value, publisher))
                    continue;
                throw publisher.Cause;
            }
        }

        private interface ISubscriptionState
        {
            long Demand { get; }
        }

        private sealed class PassThrough : ISubscriptionState
        {
            public static PassThrough Instance { get; } = new PassThrough();

            private PassThrough() { }

            public long Demand { get; } = 0;
        }

        private sealed class Buffering : ISubscriptionState
        {
            public Buffering(long demand)
            {
                Demand = demand;
            }

            public long Demand { get; }
        }

        private sealed class WrappedSubscription : AtomicReference<ISubscriptionState>, ISubscription
        {
            private static readonly Buffering NoBufferedDemand = new Buffering(0);
            private readonly ISubscription _real;
            private readonly VirtualProcessor<T> _processor;

            public WrappedSubscription(ISubscription real, VirtualProcessor<T> processor) : base(NoBufferedDemand)
            {
                _real = real;
                _processor = processor;
            }

            public void Request(long n)
            {
                if (n < 1)
                {
                    ReactiveStreamsCompliance.TryCancel(_real);
                    var value = _processor.GetAndSet(Inert.Instance);
                    if (value is Both both)
                        ReactiveStreamsCompliance.RejectDueToNonPositiveDemand(both.Subscriber);
                    else if (value == Inert.Instance)
                    {
                        // another failure has won the race
                    }
                    else
                    {
                        // this cannot possibly happen, but signaling errors is impossible at this point
                    }
                }
                else
                {
                    // NOTE: At this point, batched requests might not have been dispatched, i.e. this can reorder requests.
                    // This does not violate the Spec though, since we are a "Processor" here and although we, in reality,
                    // proxy downstream requests, it is virtually *us* that emit the requests here and we are free to follow
                    // any pattern of emitting them.
                    // The only invariant we need to keep is to never emit more requests than the downstream emitted so far.

                    while (true)
                    {
                        var current = Value;
                        if (current == PassThrough.Instance)
                        {
                            _real.Request(n);
                            break;
                        }
                        if (!CompareAndSet(current, new Buffering(current.Demand + n)))
                            continue;
                        break;
                    }
                }
            }

            public void Cancel()
            {
                _processor.Value = Inert.Instance;
                _real.Cancel();
            }

            public void UngateDemandAndRequestBuffered()
            {
                // Ungate demand
                var requests = GetAndSet(PassThrough.Instance).Demand;
                // And request buffered demand
                if (requests > 0)
                    _real.Request(requests);
            }
        }

        public override string ToString() => $"VirtualProcessor({GetHashCode()})";
    }

    /// <summary>
    /// INTERNAL API
    /// 
    /// The implementation of <see cref="Sink.AsPublisher{T}"/> needs to offer a <see cref="IPublisher{T}"/> that
    /// defers to the upstream that is connected during materialization. This would
    /// be trivial if it were not for materialized value computations that may even
    /// spawn the code that does <see cref="IPublisher{T}.Subscribe"/> in a Future, running concurrently
    /// with the actual materialization. Therefore we implement a minimal shell here
    /// that plugs the downstream and the upstream together as soon as both are known.
    /// Using a VirtualProcessor would technically also work, but it would defeat the
    /// purpose of subscription timeouts�the subscription would always already be
    /// established from the Actor�s perspective, regardless of whether a downstream
    /// will ever be connected.
    /// 
    /// One important consideration is that this <see cref="IPublisher{T}"/> must not retain a reference
    /// to the <see cref="ISubscriber{T}"/> after having hooked it up with the real <see cref="IPublisher{T}"/>, hence
    /// the use of `Inert.subscriber` as a tombstone.
    /// </summary>
    internal sealed class VirtualPublisher<T> : AtomicReference<object>, IPublisher<T>, IUntypedVirtualPublisher
    {
        #region internal classes

        private sealed class Inert
        {
            public static readonly ISubscriber<T> Subscriber = new CancellingSubscriber<T>();

            public static readonly Inert Instance = new Inert();

            private Inert()
            {
            }
        }

        #endregion

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="subscriber">TBD</param>
        public void Subscribe(ISubscriber<T> subscriber)
        {
            ReactiveStreamsCompliance.RequireNonNullSubscriber(subscriber);

            while (true)
            {
                if (Value == null)
                {
                    if (!CompareAndSet(null, subscriber))
                        continue;
                    return;
                }

                if (Value is IPublisher<T> publisher)
                {
                    if (CompareAndSet(publisher, Inert.Subscriber))
                    {
                        publisher.Subscribe(subscriber);
                        return;
                    }
                    continue;
                }

                ReactiveStreamsCompliance.RejectAdditionalSubscriber(subscriber, "Sink.AsPublisher(fanout = false)");
                return;
            }
        }

        void IUntypedVirtualPublisher.Subscribe(IUntypedSubscriber subscriber)
            => Subscribe(UntypedSubscriber.ToTyped<T>(subscriber));

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="publisher">TBD</param>
        public void RegisterPublisher(IUntypedPublisher publisher)
            => RegisterPublisher(UntypedPublisher.ToTyped<T>(publisher));

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="publisher">TBD</param>
        /// <exception cref="IllegalStateException">TBD</exception>
        public void RegisterPublisher(IPublisher<T> publisher)
        {
            while (true)
            {
                if (Value == null)
                {
                    if (!CompareAndSet(null, publisher))
                        continue;
                    return;
                }

                if (Value is ISubscriber<T> subscriber)
                {
                    Value = Inert.Instance;
                    publisher.Subscribe(subscriber);
                    return;
                }

                throw new IllegalStateException("internal error");
            }
        }

        public override string ToString() => $"VirtualProcessor(state = ${Value})";
    }


    /// <summary>
    /// TBD
    /// </summary>
    internal interface IProcessorModule
    {
        /// <summary>
        /// TBD
        /// </summary>
        Inlet In { get; }

        /// <summary>
        /// TBD
        /// </summary>
        Outlet Out { get; }

        /// <summary>
        /// TBD
        /// </summary>
        Tuple<object, object> CreateProcessor();
    }

    /// <summary>
    /// INTERNAL API
    /// </summary>
    /// <typeparam name="TIn">TBD</typeparam>
    /// <typeparam name="TOut">TBD</typeparam>
    /// <typeparam name="TMat">TBD</typeparam>
    [InternalApi]
    public sealed class ProcessorModule<TIn, TOut, TMat> : IAtomicModule<FlowShape<TIn, TOut>>, IProcessorModule
    {
        private readonly Func<Tuple<IProcessor<TIn, TOut>, TMat>> _createProcessor;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="createProcessor">TBD</param>
        /// <param name="attributes">TBD</param>
        public ProcessorModule(Func<Tuple<IProcessor<TIn, TOut>, TMat>> createProcessor, Attributes attributes = null)
        {
            _createProcessor = createProcessor;
            Attributes = attributes ?? DefaultAttributes.Processor;
            Shape = new FlowShape<TIn, TOut>((Inlet<TIn>)In, (Outlet<TOut>)Out);
            Builder = LinearTraversalBuilder.FromModule(this);
        }

        /// <summary>
        /// TBD
        /// </summary>
        public Inlet In { get; } = new Inlet<TIn>("ProcessorModule.in");

        /// <summary>
        /// TBD
        /// </summary>
        public Outlet Out { get; } = new Outlet<TOut>("ProcessorModule.out");
        
        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public Tuple<object, object> CreateProcessor()
        {
            var result = _createProcessor();
            return Tuple.Create<object, object>(result.Item1, result.Item2);
        }

        public Attributes Attributes { get; }

        public FlowShape<TIn, TOut> Shape { get; }

        public ITraversalBuilder Builder { get; } 

        public IGraph<FlowShape<TIn, TOut>> WithAttributes(Attributes attributes) =>
            new ProcessorModule<TIn, TOut, TMat>(_createProcessor, attributes);

        public IGraph<FlowShape<TIn, TOut>> AddAttributes(Attributes attributes) =>
            WithAttributes(Builder.Attributes.And(attributes));

        public IGraph<FlowShape<TIn, TOut>> Named(string name) => AddAttributes(Attributes.CreateName(name));

        public IGraph<FlowShape<TIn, TOut>> Async() => AddAttributes(Attributes.CreateAsyncBoundary());
    }
}
