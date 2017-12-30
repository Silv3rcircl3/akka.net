﻿//-----------------------------------------------------------------------
// <copyright file="Retry.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2017 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2017 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Pattern;
using Akka.Streams.Stage;
using Akka.Util;

namespace Akka.Streams.Dsl
{
    public static class Retry
    {
        /// <summary>
        /// EXPERIMENTAL API
        /// <para>
        /// Retry flow factory. given a flow that produces <see cref="Result{T}"/>s, this wrapping flow may be used to try
        /// and pass failed elements through the flow again. More accurately, the given flow consumes a tuple
        /// of `input` and `state`, and produces a tuple of <see cref="Result{T}"/> of `output` and `state`.
        /// If the flow emits a failed element (i.e. <see cref="Result{T}.IsSuccess"/> is false), the <paramref name="retryWith"/>
        /// function is fed with the `state` of the failed element, and may produce a new input-state tuple to pass through
        /// the original flow. The function may also yield `null` instead of `(input, state)`, which means not to retry a failed element.
        /// </para>
        /// <para>
        /// IMPORTANT CAVEAT:
        /// The given flow must not change the number of elements passing through it (i.e. it should output
        /// exactly one element for every received element). Ignoring this, will have an unpredicted result,
        /// and may result in a deadlock.
        /// </para> 
        /// </summary>
        /// <param name="flow">the flow to retry</param>
        /// <param name="retryWith">if output was failure, we can optionaly recover from it,
        /// and retry with a new pair of input and new state we get from this function.</param>
        /// <typeparam name="TI">input elements type</typeparam>
        /// <typeparam name="TS">state to create a new `(I,S)` to retry with</typeparam>
        /// <typeparam name="TO">output elements type</typeparam>
        /// <typeparam name="TM">materialized value type</typeparam>
        public static IGraph<FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>, TM> Create<TI, TS, TO, TM>(
            IGraph<FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>, TM> flow, Func<TS, Tuple<TI, TS>> retryWith)
        {
            return GraphDsl.Create(flow, (b, origFlow) =>
            {
                var retry = b.Add(new RetryCoordinator<TI, TS, TO>(retryWith));

                b.From(retry.Outlet2).Via(origFlow).To(retry.Inlet2);

                return new FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>(retry.Inlet1, retry.Outlet1);
            });
        }

        /// <summary>
        /// EXPERIMENTAL API
        /// <para>
        /// Factory for multiple retries flow. similar to the simple retry, but this will allow to
        /// break down a "heavy" element which failed into multiple "thin" elements, that may succeed individually.
        /// Since it's easy to inflate elements in retry cycle, there's also a limit parameter, that will limit the
        /// amount of generated elements by the `retryWith` function, and will fail the stage if that limit is exceeded.
        /// </para>
        /// <para>
        /// Passing `null` is valid, and will result in filtering out the failure quietly, without
        /// emitting a failed <see cref="Result{T}"/> element.
        /// </para>
        /// <para>
        /// IMPORTANT CAVEAT:
        /// The given flow must not change the number of elements passing through it (i.e. it should output
        ///     exactly one element for every received element). Ignoring this, will have an unpredicted result,
        /// and may result in a deadlock.
        /// </para>
        /// </summary>
        /// <param name="limit">since every retry can generate more elements, the inner queue can get too big.
        /// if the limit is reached, the stage will fail.</param>
        /// <param name="flow">the flow to retry</param>
        /// <param name="retryWith">if output was failure, we can optionaly recover from it, and retry with
        /// a sequence of input and new state pairs we get from this function.</param>
        /// <typeparam name="TI">input elements type</typeparam>
        /// <typeparam name="TS">state to create a new `(I,S)` to retry with</typeparam>
        /// <typeparam name="TO">output elements type</typeparam>
        /// <typeparam name="TM">materialized value type</typeparam>
        /// <returns></returns>
        public static IGraph<FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>, TM> Concat<TI, TS, TO, TM>(long limit,
            IGraph<FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>, TM> flow, Func<TS, IEnumerable<Tuple<TI, TS>>> retryWith)
        {
            return GraphDsl.Create(flow, (b, origFlow) =>
            {
                var retry = b.Add(new RetryConcatCoordinator<TI, TS, TO>(limit, retryWith));

                b.From(retry.Outlet2).Via(origFlow).To(retry.Inlet2);

                return new FlowShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>>(retry.Inlet1, retry.Outlet1);
            });
        }


        private class RetryCoordinator<TI, TS, TO> : GraphStage<BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>>>
        {
            #region Logic

            private sealed class Logic : GraphStageLogic
            {
                private readonly RetryCoordinator<TI, TS, TO> _retry;
                private bool _elementInCycle;
                private Tuple<TI, TS> _pending;

                public Logic(RetryCoordinator<TI, TS, TO> retry) : base(retry.Shape)
                {

                    _retry = retry;

                    SetHandler(retry.In1, onPush: () =>
                    {
                        var item = Grab(retry.In1);
                        if (!HasBeenPulled(retry.In2))
                            Pull(retry.In2);
                        Push(retry.Out2, item);
                        _elementInCycle = true;
                    }, onUpstreamFinish: () =>
                    {
                        if (!_elementInCycle)
                            CompleteStage();
                    });

                    SetHandler(retry.Out1, onPull: () =>
                    {
                        if (IsAvailable(retry.Out2))
                            Pull(retry.In1);
                        else
                            Pull(retry.In2);
                    });

                    SetHandler(retry.In2, onPush: () =>
                    {
                        _elementInCycle = false;
                        var t = Grab(retry.In2);
                        var result = t.Item1;

                        if (result.IsSuccess)
                            PushAndCompleteIfLast(t);
                        else
                        {
                            var r = retry._retryWith(t.Item2);
                            if (r == null)
                                PushAndCompleteIfLast(t);
                            else
                            {
                                Pull(retry.In2);
                                if (IsAvailable(retry.Out2))
                                {
                                    Push(retry.Out2, r.Item2);
                                    _elementInCycle = true;
                                }
                                else
                                    _pending = r;
                            }

                        }
                    });

                    SetHandler(retry.Out2, onPull: () =>
                    {
                        if (IsAvailable(retry.Out1) && !_elementInCycle)
                        {
                            if (_pending != null)
                            {
                                Push(retry.Out2, _pending);
                                _pending = null;
                                _elementInCycle = true;
                            }
                            else if (!HasBeenPulled(retry.In1))
                                Pull(retry.In1);
                        }
                    }, onDownstreamFinish: () =>
                    {
                        //Do Nothing, intercept completion as downstream
                    });
                }

                private void PushAndCompleteIfLast(Tuple<Result<TO>, TS> item)
                {
                    Push(_retry.Out1, item);
                    if (IsClosed(_retry.In1))
                        CompleteStage();
                }
            }

            #endregion

            private readonly Func<TS, Tuple<TI, TS>> _retryWith;

            public RetryCoordinator(Func<TS, Tuple<TI, TS>> retryWith)
            {
                _retryWith = retryWith;

                In1 = new Inlet<Tuple<TI, TS>>("Retry.ext.in");
                Out1 = new Outlet<Tuple<Result<TO>, TS>>("Retry.ext.out");
                In2 = new Inlet<Tuple<Result<TO>, TS>>("Retry.int.in");
                Out2 = new Outlet<Tuple<TI, TS>>("Retry.int.out");
                Shape = new BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>>(In1, Out1, In2, Out2);
            }

            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

            public override BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>> Shape { get; }

            public Inlet<Tuple<TI, TS>> In1 { get; }
            public Outlet<Tuple<Result<TO>, TS>> Out1 { get; }
            public Inlet<Tuple<Result<TO>, TS>> In2 { get; }
            public Outlet<Tuple<TI, TS>> Out2 { get; }
        }


        private class RetryConcatCoordinator<TI, TS, TO> : GraphStage<BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>>>
        {
            #region Logic

            private sealed class Logic : GraphStageLogic
            {
                private readonly RetryConcatCoordinator<TI, TS, TO> _retry;
                private readonly Queue<Tuple<TI, TS>> _queue = new Queue<Tuple<TI, TS>>();
                private bool _elementInCycle;

                public Logic(RetryConcatCoordinator<TI, TS, TO> retry) : base(retry.Shape)
                {
                    _retry = retry;

                    SetHandler(retry.In1, onPush: () =>
                    {
                        var item = Grab(retry.In1);
                        if (!HasBeenPulled(retry.In2))
                            Pull(retry.In2);
                        if (IsAvailable(retry.Out2))
                        {
                            Push(retry.Out2, item);
                            _elementInCycle = true;
                        }
                        else
                            _queue.Enqueue(item);
                    }, onUpstreamFinish: () =>
                    {
                        if (!_elementInCycle && _queue.Count == 0)
                            CompleteStage();
                    });

                    SetHandler(retry.Out1, onPull: () =>
                    {
                        if (_queue.Count == 0)
                        {
                            if (IsAvailable(retry.Out2))
                                Pull(retry.In1);
                            else
                                Pull(retry.In2);
                        }
                        else
                        {
                            Pull(retry.In2);
                            if (IsAvailable(retry.Out2))
                            {
                                Push(retry.Out2, _queue.Dequeue());
                                _elementInCycle = true;
                            }
                        }
                    });

                    SetHandler(retry.In2, onPush: () =>
                    {
                        _elementInCycle = false;
                        var t = Grab(retry.In2);
                        var result = t.Item1;

                        if (result.IsSuccess)
                            PushAndCompleteIfLast(t);
                        else
                        {
                            var r = retry._retryWith(t.Item2);
                            if (r == null)
                                PushAndCompleteIfLast(t);
                            else
                            {
                                var items = r.ToList();
                                if (items.Count + _queue.Count > retry._limit)
                                    FailStage(new IllegalStateException($"Queue limit of {retry._limit} has been exceeded. Trying to append {items.Count} elements to a queue that has {_queue.Count} elements."));
                                else
                                {
                                    foreach (var i in items)
                                        _queue.Enqueue(i);

                                    if (_queue.Count == 0)
                                    {
                                        if (IsClosed(retry.In1))
                                            CompleteStage();
                                        else
                                            Pull(retry.In1);
                                    }
                                    else
                                    {
                                        Pull(retry.In2);
                                        if (IsAvailable(retry.Out2))
                                        {
                                            Push(retry.Out2, _queue.Dequeue());
                                            _elementInCycle = true;
                                        }
                                    }
                                }
                            }

                        }
                    });

                    SetHandler(retry.Out2, onPull: () =>
                    {
                        if (!_elementInCycle && IsAvailable(retry.Out1))
                        {
                            if (_queue.Count == 0)
                                Pull(_retry.In1);
                            else
                            {
                                Push(retry.Out2, _queue.Dequeue());
                                _elementInCycle = true;
                                if (!HasBeenPulled(_retry.In2))
                                    Pull(_retry.In2);
                            }
                        }
                    }, onDownstreamFinish: () =>
                    {
                        //Do Nothing, intercept completion as downstream
                    });
                }

                private void PushAndCompleteIfLast(Tuple<Result<TO>, TS> item)
                {
                    Push(_retry.Out1, item);
                    if (IsClosed(_retry.In1) && _queue.Count == 0)
                        CompleteStage();
                }
            }

            #endregion

            private readonly long _limit;
            private readonly Func<TS, IEnumerable<Tuple<TI, TS>>> _retryWith;

            public RetryConcatCoordinator(long limit, Func<TS, IEnumerable<Tuple<TI, TS>>> retryWith)
            {
                _limit = limit;
                _retryWith = retryWith;

                In1 = new Inlet<Tuple<TI, TS>>("RetryConcat.ext.in");
                Out1 = new Outlet<Tuple<Result<TO>, TS>>("RetryConcat.ext.out");
                In2 = new Inlet<Tuple<Result<TO>, TS>>("RetryConcat.int.in");
                Out2 = new Outlet<Tuple<TI, TS>>("RetryConcat.int.out");
                Shape = new BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>>(In1, Out1, In2, Out2);
            }

            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

            public override BidiShape<Tuple<TI, TS>, Tuple<Result<TO>, TS>, Tuple<Result<TO>, TS>, Tuple<TI, TS>> Shape { get; }

            public Inlet<Tuple<TI, TS>> In1 { get; }
            public Outlet<Tuple<Result<TO>, TS>> Out1 { get; }
            public Inlet<Tuple<Result<TO>, TS>> In2 { get; }
            public Outlet<Tuple<TI, TS>> Out2 { get; }
        }
    }
}