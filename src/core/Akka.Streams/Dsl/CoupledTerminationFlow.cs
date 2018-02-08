//-----------------------------------------------------------------------
// <copyright file="CoupledTerminationFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------
using System;
using Akka.Streams.Stage;

namespace Akka.Streams.Dsl
{
    /// <summary>
    /// Allows coupling termination (cancellation, completion, erroring) of Sinks and Sources while creating a Flow them them.
    /// Similar to <see cref="Flow.FromSinkAndSource{TIn,TOut,TMat}"/> however that API does not connect the completion signals of the wrapped stages.
    /// </summary>
    public static class CoupledTerminationFlow
    {
        /// <summary>
        /// Similar to <see cref="Flow.FromSinkAndSource{TIn,TOut,TMat}"/> however couples the termination of these two stages.
        ///
        /// E.g. if the emitted <see cref="Flow{TIn,TOut,TMat}"/> gets a cancellation, the <see cref="Source{TOut,TMat}"/> of course is cancelled,
        /// however the <see cref="Sink{TIn,TMat}"/> will also be completed.
        /// 
        /// <table>
        ///   <tr>
        ///     <th>Returned Flow</th>
        ///     <th>Sink (<code>in</code>)</th>
        ///     <th>Source(<code>out</code>)</th>
        ///   </tr>
        ///   <tr>
        ///     <td><i>cause:</i> upstream(sink-side) receives completion</td>
        ///     <td><i>effect:</i> receives completion</td>
        ///     <td><i>effect:</i> receives cancel</td>
        ///   </tr>
        ///   <tr>
        ///     <td><i>cause:</i> upstream (sink-side) receives error</td>
        ///     <td><i>effect:</i> receives error</td>
        ///     <td><i>effect:</i> receives cancel</td>
        ///   </tr>
        ///   <tr>
        ///     <td><i>cause:</i> downstream (source-side) receives cancel</td>
        ///     <td><i>effect:</i> completes</td>
        ///     <td><i>effect:</i> receives cancel</td>
        ///   </tr>
        ///   <tr>
        ///     <td><i>effect:</i> cancels upstream, completes downstream</td>
        ///     <td><i>effect:</i> completes</td>
        ///     <td><i>cause:</i> signals complete</td>
        ///   </tr>
        ///   <tr>
        ///     <td><i>effect:</i> cancels upstream, errors downstream</td>
        ///     <td><i>effect:</i> receives error</td>
        ///     <td><i>cause:</i> signals error or throws</td>
        ///   </tr>
        ///   <tr>
        ///     <td><i>effect:</i> cancels upstream, completes downstream</td>
        ///     <td><i>cause:</i> cancels</td>
        ///     <td><i>effect:</i> receives cancel</td>
        ///   </tr>
        /// </table>
        /// 
        /// The order in which the "in" and "out" sides receive their respective completion signals is not defined, do not rely on its ordering.
        /// </summary>
        public static Flow<TIn, TOut, Tuple<TMat1, TMat2>> FromSinkAndSource<TIn, TOut, TMat1, TMat2>(
            Sink<TIn, TMat1> @in, Source<TOut, TMat2> @out)
        {
            return Flow.FromGraph(GraphDsl.Create(@in, @out, Keep.Both, (b, i, o) =>
            {
                var bidi = b.Add(new CoupledTerminationBidi<TIn, TOut>());

                /* bidi.in1 -> */
                b.From(bidi.Outlet1).To(i);
                b.From(o).To(bidi.Inlet2); /* -> bidi.out2  */

                return new FlowShape<TIn, TOut>(bidi.Inlet1, bidi.Outlet2);
            }));
        }

        /// <summary>
        /// INTERNAL API
        /// </summary>
        private sealed class CoupledTerminationBidi<T1, T2> : GraphStage<BidiShape<T1, T1, T2, T2>>
        {
            private sealed class Logic : GraphStageLogic
            {
                public Logic(CoupledTerminationBidi<T1, T2> stage) : base(stage.Shape)
                {
                    SetHandler(stage._in1, () => Push(stage._out1, Grab(stage._in1)), CompleteStage, FailStage);
                    SetHandler(stage._out1, () => Pull(stage._in1), CompleteStage);
                    SetHandler(stage._in2, () => Push(stage._out2, Grab(stage._in2)), CompleteStage, FailStage);
                    SetHandler(stage._out2, () => Pull(stage._in2), CompleteStage);
                }
            }

            public CoupledTerminationBidi() => Shape = new BidiShape<T1, T1, T2, T2>(_in1, _out1, _in2, _out2);

            private readonly Inlet<T1> _in1 = new Inlet<T1>("CoupledCompletion.in1");
            private readonly Outlet<T1> _out1 = new Outlet<T1>("CoupledCompletion.out1");
            private readonly Inlet<T2> _in2 = new Inlet<T2>("CoupledCompletion.in2");
            private readonly Outlet<T2> _out2 = new Outlet<T2>("CoupledCompletion.out2");

            public override BidiShape<T1, T1, T2, T2> Shape { get; }

            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
        }
    }
}
