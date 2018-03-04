//-----------------------------------------------------------------------
// <copyright file="BidiFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Streams.Implementation;

namespace Akka.Streams.Dsl
{
    /// <summary>
    /// TBD
    /// </summary>
    public static class BidiFlow
    {
        /// <summary>
        /// A graph with the shape of a flow logically is a flow, this method makes
        /// it so also in type.
        /// </summary>
        /// <typeparam name="TIn1">TBD</typeparam>
        /// <typeparam name="TOut1">TBD</typeparam>
        /// <typeparam name="TIn2">TBD</typeparam>
        /// <typeparam name="TOut2">TBD</typeparam>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="graph">TBD</param>
        /// <returns>TBD</returns>
        public static BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat> FromGraph<TIn1, TOut1, TIn2, TOut2, TMat>(
            IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat> graph)
        {
            return graph is BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat> flow
                ? flow
                : new BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat>(graph.Builder, graph.Shape);
        }

        /// <summary>
        /// Wraps two Flows to create a <see cref="BidiFlow{TIn1,TOut1,TIn2,TOut2,TMat}"/>. The materialized value of the resulting BidiFlow is determined
        /// by the combiner function passed in the second argument list.
        /// <![CDATA[ 
        /// {{{
        ///     +----------------------------+
        ///     | Resulting BidiFlow         |
        ///     |                            |
        ///     |  +----------------------+  |
        /// I1 ~~> |        Flow1         | ~~> O1
        ///     |  +----------------------+  |
        ///     |                            |
        ///     |  +----------------------+  |
        /// O2 \<~~ |        Flow2         | <~~ I2
        ///     |  +----------------------+  |
        ///     +----------------------------+
        /// }}}
        /// ]]>
        /// </summary>
        /// <typeparam name="TIn1">TBD</typeparam>
        /// <typeparam name="TOut1">TBD</typeparam>
        /// <typeparam name="TIn2">TBD</typeparam>
        /// <typeparam name="TOut2">TBD</typeparam>
        /// <typeparam name="TMat1">TBD</typeparam>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="flow1">TBD</param>
        /// <param name="flow2">TBD</param>
        /// <param name="combine">TBD</param>
        /// <returns>TBD</returns>
        public static BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat> FromFlowsMat
            <TIn1, TOut1, TIn2, TOut2, TMat1, TMat2, TMat>(IGraph<FlowShape<TIn1, TOut1>, TMat1> flow1,
                IGraph<FlowShape<TIn2, TOut2>, TMat2> flow2, Func<TMat1, TMat2, TMat> combine)
        {
            var newFlow1Shape = flow1.Shape.DeepCopy();
            var newFlow2Shape = flow2.Shape.DeepCopy();

            return new BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat>(
                TraversalBuilder.Empty()
                    .Add(flow1.Builder, newFlow1Shape)
                    .Add(flow2.Builder, newFlow2Shape, combine),
                new BidiShape<TIn1, TOut1, TIn2, TOut2>((Inlet<TIn1>)newFlow1Shape.Inlets[0],
                    (Outlet<TOut1>)newFlow1Shape.Outlets[0], (Inlet<TIn2>)newFlow2Shape.Inlets[0],
                    (Outlet<TOut2>)newFlow2Shape.Outlets[0]));
        }

        /// <summary>
        /// Wraps two Flows to create a <see cref="BidiFlow{TIn1,TOut1,TIn2,TOut2,TMat}"/>. The materialized value of the resulting BidiFlow is NotUsed.
        /// <![CDATA[ 
        /// {{{
        ///     +----------------------------+
        ///     | Resulting BidiFlow         |
        ///     |                            |
        ///     |  +----------------------+  |
        /// I1 ~~> |        Flow1         | ~~> O1
        ///     |  +----------------------+  |
        ///     |                            |
        ///     |  +----------------------+  |
        /// O2 <~~ |        Flow2         | <~~ I2
        ///     |  +----------------------+  |
        ///     +----------------------------+
        /// }}}
        /// ]]>
        /// </summary>
        /// <typeparam name="TIn1">TBD</typeparam>
        /// <typeparam name="TOut1">TBD</typeparam>
        /// <typeparam name="TIn2">TBD</typeparam>
        /// <typeparam name="TOut2">TBD</typeparam>
        /// <typeparam name="TMat1">TBD</typeparam>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <param name="flow1">TBD</param>
        /// <param name="flow2">TBD</param>
        /// <returns>TBD</returns>
        public static BidiFlow<TIn1, TOut1, TIn2, TOut2, NotUsed> FromFlows<TIn1, TOut1, TIn2, TOut2, TMat1, TMat2>(
            IGraph<FlowShape<TIn1, TOut1>, TMat1> flow1, IGraph<FlowShape<TIn2, TOut2>, TMat2> flow2)
        {
            return FromFlowsMat(flow1, flow2, Keep.None);
        }

        /// <summary>
        /// Create a <see cref="BidiFlow{TIn1,TOut1,TIn2,TOut2,TMat}"/> where the top and bottom flows are just one simple mapping
        /// stage each, expressed by the two functions.
        /// </summary>
        /// <typeparam name="TIn1">TBD</typeparam>
        /// <typeparam name="TOut1">TBD</typeparam>
        /// <typeparam name="TIn2">TBD</typeparam>
        /// <typeparam name="TOut2">TBD</typeparam>
        /// <param name="outbound">TBD</param>
        /// <param name="inbound">TBD</param>
        /// <returns>TBD</returns>
        public static BidiFlow<TIn1, TOut1, TIn2, TOut2, NotUsed> FromFunction<TIn1, TOut1, TIn2, TOut2>(Func<TIn1, TOut1> outbound, Func<TIn2, TOut2> inbound)
        {
            return FromFlows(Flow.Create<TIn1>().Select(outbound), Flow.Create<TIn2>().Select(inbound));
        }

        /// <summary>
        /// If the time between two processed elements ///in any direction/// exceed the provided timeout, the stream is failed
        /// with a <see cref="TimeoutException"/>.
        ///
        /// There is a difference between this stage and having two idleTimeout Flows assembled into a BidiStage.
        /// If the timeout is configured to be 1 seconds, then this stage will not fail even though there are elements flowing
        /// every second in one direction, but no elements are flowing in the other direction. I.e. this stage considers
        /// the ///joint/// frequencies of the elements in both directions.
        /// </summary>
        /// <typeparam name="TIn">TBD</typeparam>
        /// <typeparam name="TOut">TBD</typeparam>
        /// <param name="timeout">TBD</param>
        /// <returns>TBD</returns>
        public static BidiFlow<TIn, TIn, TOut, TOut, NotUsed> BidirectionalIdleTimeout<TIn, TOut>(TimeSpan timeout)
        {
            return FromGraph(new IdleTimeoutBidi<TIn, TOut>(timeout));
        }
    }

    /// <summary>
    /// TBD
    /// </summary>
    /// <typeparam name="TIn1">TBD</typeparam>
    /// <typeparam name="TOut1">TBD</typeparam>
    /// <typeparam name="TIn2">TBD</typeparam>
    /// <typeparam name="TOut2">TBD</typeparam>
    /// <typeparam name="TMat">TBD</typeparam>
    public class BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat> : IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat>
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="builder">TBD</param>
        public BidiFlow(ITraversalBuilder builder, BidiShape<TIn1, TOut1, TIn2, TOut2> shape)
        {
            Shape = shape;
            Builder = builder;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public BidiShape<TIn1, TOut1, TIn2, TOut2> Shape { get; }

        public ITraversalBuilder Builder { get; }


        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="attributes">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat> WithAttributes(Attributes attributes)
        {
            return new BidiFlow<TIn1, TOut1, TIn2, TOut2, TMat>(Builder.SetAttributes(attributes), Shape);
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="attributes">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat> AddAttributes(Attributes attributes)
        {
            return WithAttributes(Builder.Attributes.And(attributes));
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="name">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat> Named(string name)
        {
            return AddAttributes(Attributes.CreateName(name));
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public IGraph<BidiShape<TIn1, TOut1, TIn2, TOut2>, TMat> Async()
        {
            return AddAttributes(new Attributes(Attributes.AsyncBoundary.Instance));
        }

        /// <summary>
        /// Turn this BidiFlow around by 180 degrees, logically flipping it upside down in a protocol stack.
        /// </summary>
        /// <returns>TBD</returns>
        public BidiFlow<TIn2, TOut2, TIn1, TOut1, TMat> Reversed()
        {
            return new BidiFlow<TIn2, TOut2, TIn1, TOut1, TMat>(Builder, new BidiShape<TIn2, TOut2, TIn1, TOut1>(Shape.Inlet2, Shape.Outlet2, Shape.Inlet1, Shape.Outlet1));
        }

        /// <summary>
        /// Add the given BidiFlow as the next step in a bidirectional transformation
        /// pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
        /// layer, the closest to the metal.
        /// <![CDATA[ 
        /// {{{
        ///     +----------------------------+
        ///     | Resulting BidiFlow         |
        ///     |                            |
        ///     |  +------+        +------+  |
        /// I1 ~~> |      |  ~O1~> |      | ~~> OO1
        ///     |  | this |        | bidi |  |
        /// O2 <~~ |      | <~I2~  |      | <~~ II2
        ///     |  +------+        +------+  |
        ///     +----------------------------+
        /// }}}
        /// ]]>
        /// The materialized value of the combined <see cref="BidiFlow{TIn1,TOut1,TIn2,TOut2,TMat}"/> will be the materialized
        /// value of the current flow (ignoring the other BidiFlow’s value), use
        /// <see cref="AtopMat{TOut12,TIn21,TMat2,TMat3}"/> if a different strategy is needed.
        /// </summary>
        /// <typeparam name="TOut12">TBD</typeparam>
        /// <typeparam name="TIn21">TBD</typeparam>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <param name="bidi">TBD</param>
        /// <returns>TBD</returns>
        public BidiFlow<TIn1, TOut12, TIn21, TOut2, TMat> Atop<TOut12, TIn21, TMat2>(BidiFlow<TOut1, TOut12, TIn21, TIn2, TMat2> bidi)
        {
            return AtopMat(bidi, Keep.Left);
        }

        /// <summary>
        /// Add the given BidiFlow as the next step in a bidirectional transformation
        /// pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
        /// layer, the closest to the metal.
        ///  <![CDATA[ 
        /// {{{
        ///     +----------------------------+
        ///     | Resulting BidiFlow         |
        ///     |                            |
        ///     |  +------+        +------+  |
        /// I1 ~~> |      |  ~O1~> |      | ~~> OO1
        ///     |  | this |        | bidi |  |
        /// O2 <~~ |      | <~I2~  |      | <~~ II2
        ///     |  +------+        +------+  |
        ///     +----------------------------+
        /// }}}
        ///  ]]>
        /// The <paramref name="combine"/> function is used to compose the materialized values of this flow and that
        /// flow into the materialized value of the resulting BidiFlow.
        /// </summary>
        /// <typeparam name="TOut12">TBD</typeparam>
        /// <typeparam name="TIn21">TBD</typeparam>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <typeparam name="TMat3">TBD</typeparam>
        /// <param name="bidi">TBD</param>
        /// <param name="combine">TBD</param>
        /// <returns>TBD</returns>
        public BidiFlow<TIn1, TOut12, TIn21, TOut2, TMat> AtopMat<TOut12, TIn21, TMat2, TMat3>(BidiFlow<TOut1, TOut12, TIn21, TIn2, TMat2> bidi, Func<TMat, TMat2, TMat3> combine)
        {
            var newBidiShape = bidi.Shape.DeepCopy();

            return new BidiFlow<TIn1, TOut12, TIn21, TOut2, TMat>(
                Builder.Add(bidi.Builder, newBidiShape, combine)
                .Wire(Shape.Outlet1, newBidiShape.Inlets[0])
                .Wire(newBidiShape.Outlets[1], Shape.Inlet2),
                new BidiShape<TIn1, TOut12, TIn21, TOut2>(Shape.Inlet1, (Outlet<TOut12>)newBidiShape.Outlets[0], (Inlet<TIn21>)newBidiShape.Inlets[1], Shape.Outlet2));
        }

        /// <summary>
        /// Add the given Flow as the final step in a bidirectional transformation
        /// pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
        /// layer, the closest to the metal.
        /// <![CDATA[ 
        /// {{{
        ///     +---------------------------+
        ///     | Resulting Flow            |
        ///     |                           |
        ///     |  +------+        +------+ |
        /// I1 ~~> |      |  ~O1~> |      | |
        ///     |  | this |        | flow | |
        /// O2 <~~ |      | <~I2~  |      | |
        ///     |  +------+        +------+ |
        ///     +---------------------------+
        /// }}}
        /// ]]>
        /// The materialized value of the combined <see cref="Flow{TIn,TOut,TMat}"/> will be the materialized
        /// value of the current flow (ignoring the other Flow’s value), use
        /// <see cref="JoinMat{TMat2,TMat3}"/> if a different strategy is needed.
        /// </summary>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <param name="flow">TBD</param>
        /// <returns>TBD</returns>
        public Flow<TIn1, TOut2, TMat> Join<TMat2>(Flow<TOut1, TIn2, TMat2> flow)
        {
            return JoinMat(flow, Keep.Left);
        }

        /// <summary>
        /// Add the given Flow as the final step in a bidirectional transformation
        /// pipeline. By convention protocol stacks are growing to the left: the right most is the bottom
        /// layer, the closest to the metal.
        /// <![CDATA[ 
        /// {{{
        ///     +---------------------------+
        ///     | Resulting Flow            |
        ///     |                           |
        ///     |  +------+        +------+ |
        /// I1 ~~> |      |  ~O1~> |      | |
        ///     |  | this |        | flow | |
        /// O2 <~~ |      | <~I2~  |      | |
        ///     |  +------+        +------+ |
        ///     +---------------------------+
        /// }}}
        /// ]]>
        /// The <paramref name="combine"/> function is used to compose the materialized values of this flow and that
        /// flow into the materialized value of the resulting <see cref="Flow{TIn,TOut,TMat}"/>.
        /// </summary>
        /// <typeparam name="TMat2">TBD</typeparam>
        /// <typeparam name="TMat3">TBD</typeparam>
        /// <param name="flow">TBD</param>
        /// <param name="combine">TBD</param>
        /// <returns>TBD</returns>
        public Flow<TIn1, TOut2, TMat3> JoinMat<TMat2, TMat3>(Flow<TOut1, TIn2, TMat2> flow, Func<TMat, TMat2, TMat3> combine)
        {
            var newFlowSHape = flow.Shape.DeepCopy();

            var resultBuilder = Builder
                .Add(flow.Builder, newFlowSHape, combine)
                .Wire(Shape.Outlet1, newFlowSHape.Inlets[0])
                .Wire(newFlowSHape.Outlets[0], Shape.Inlet2);

            var newShape = new FlowShape<TIn1, TOut2>(Shape.Inlet1, Shape.Outlet2);

            return new Flow<TIn1, TOut2, TMat3>(
                LinearTraversalBuilder.FromBuilder<TMat, TMat2, TMat2>(resultBuilder, newShape, Keep.Right),
                newShape);
        }
    }
}