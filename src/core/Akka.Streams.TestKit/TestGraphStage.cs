//-----------------------------------------------------------------------
// <copyright file="TestGraphStage.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.TestKit;

namespace Akka.Streams.TestKit
{
    /// <summary>
    /// Messages emitted after the corresponding `stageUnderTest` methods has been invoked.
    /// </summary>
    public static class GraphStageMessages
    {
        public interface IStageMessage { }

        public class Push : INoSerializationVerificationNeeded, IStageMessage
        {
            public static Push Instance { get; } = new Push();

            private Push() { }
        }

        public class UpstreamFinish : INoSerializationVerificationNeeded, IStageMessage
        {
            public static UpstreamFinish Instance { get; } = new UpstreamFinish();

            private UpstreamFinish() { }
        }

        public class Failure : INoSerializationVerificationNeeded, IStageMessage
        {
            public Failure(Exception ex)
            {
                Ex = ex;
            }

            public Exception Ex { get; }
        }

        public class Pull : INoSerializationVerificationNeeded, IStageMessage
        {
            public static Pull Instance { get; } = new Pull();

            private Pull() { }
        }

        public class DownstreamFinish : INoSerializationVerificationNeeded, IStageMessage
        {
            public static DownstreamFinish Instance { get; } = new DownstreamFinish();

            private DownstreamFinish() { }
        }

        /// <summary>
        /// Sent to the probe when the stage callback threw an exception
        /// </summary>
        public class StageFailure
        {
            public IStageMessage Operation { get; }
            public Exception Exception { get; }

            public StageFailure(IStageMessage operation, Exception exception)
            {
                Operation = operation;
                Exception = exception;
            }
        }
    }

    public sealed class TestSinkStage<T, TMat> : GraphStageWithMaterializedValue<SinkShape<T>, TMat>
    {
        /// <summary>
        /// Creates a sink out of the `stageUnderTest` that will inform the `probe`
        /// of graph stage events and callbacks by sending it the various messages found under
        /// <see cref="GraphStageMessages"/>
        /// 
        /// This allows for creation of a "normal" stream ending with the sink while still being
        /// able to assert internal events.
        /// </summary>
        public static TestSinkStage<T, TMat> Create(GraphStageWithMaterializedValue<SinkShape<T>, TMat> stageUnderTest,
            TestProbe probe) => new TestSinkStage<T, TMat>(stageUnderTest, probe);

        private readonly Inlet<T> _in = new Inlet<T>("testSinkStage.in");
        private readonly GraphStageWithMaterializedValue<SinkShape<T>, TMat> _stageUnderTest;
        private readonly TestProbe _probe;

        private TestSinkStage(GraphStageWithMaterializedValue<SinkShape<T>, TMat> stageUnderTest, TestProbe probe)
        {
            _stageUnderTest = stageUnderTest;
            _probe = probe;
            Shape = new SinkShape<T>(_in);
        }

        public override SinkShape<T> Shape { get; }

        public override ILogicAndMaterializedValue<TMat> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            _stageUnderTest.Shape.Inlet.Id = _in.Id;
            var logicAndMaterialized = _stageUnderTest.CreateLogicAndMaterializedValue(inheritedAttributes);
            var logic = logicAndMaterialized.Logic;

            var inHandler = (IInHandler)logic.Handlers[_in.Id];
            logic.SetHandler(_in, onPush: () =>
            {
                try
                {
                    inHandler.OnPush();
                    _probe.Ref.Tell(GraphStageMessages.Push.Instance);
                }
                catch (Exception e)
                {
                    _probe.Ref.Tell(new GraphStageMessages.StageFailure(GraphStageMessages.Push.Instance, e));
                    throw;
                }
            }, onUpstreamFinish: () =>
            {
                try
                {
                    inHandler.OnUpstreamFinish();
                    _probe.Ref.Tell(GraphStageMessages.UpstreamFinish.Instance);
                }
                catch (Exception e)
                {
                    _probe.Ref.Tell(new GraphStageMessages.StageFailure(GraphStageMessages.UpstreamFinish.Instance, e));
                    throw;
                }
            }, onUpstreamFailure: e =>
            {
                try
                {
                    inHandler.OnUpstreamFailure(e);
                    _probe.Ref.Tell(new GraphStageMessages.Failure(e));
                }
                catch (Exception ex)
                {
                    _probe.Ref.Tell(new GraphStageMessages.StageFailure(new GraphStageMessages.Failure(ex), ex));
                    throw;
                }
            });

            return logicAndMaterialized;
        }
    }

    public sealed class TestSourceStage<T, TMat> : GraphStageWithMaterializedValue<SourceShape<T>, TMat>
    {
        /// <summary>
        /// Creates a source out of the `stageUnderTest` that will inform the `probe`
        /// of graph stage events and callbacks by sending it the various messages found under
        /// <see cref="GraphStageMessages"/>
        /// 
        /// This allows for creation of a "normal" stream starting with the source while still being
        /// able to assert internal events.
        /// </summary>
        public static Source<T, TMat> Create(GraphStageWithMaterializedValue<SourceShape<T>, TMat> stageUnderTest,
            TestProbe probe) => Source.FromGraph(new TestSourceStage<T, TMat>(stageUnderTest, probe));

        private readonly Outlet<T> _out = new Outlet<T>("testSourceStage.out");
        private readonly GraphStageWithMaterializedValue<SourceShape<T>, TMat> _stageUnderTest;
        private readonly TestProbe _probe;

        private TestSourceStage(GraphStageWithMaterializedValue<SourceShape<T>, TMat> stageUnderTest, TestProbe probe)
        {
            _stageUnderTest = stageUnderTest;
            _probe = probe;
            Shape = new SourceShape<T>(_out);
        }

        public override SourceShape<T> Shape { get; }

        public override ILogicAndMaterializedValue<TMat> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            _stageUnderTest.Shape.Outlet.Id = _out.Id;
            var logicAndMaterialized = _stageUnderTest.CreateLogicAndMaterializedValue(inheritedAttributes);
            var logic = logicAndMaterialized.Logic;

            var outHandler = (IOutHandler)logic.Handlers[_out.Id];
            logic.SetHandler(_out, onPull: () =>
            {
                try
                {
                    outHandler.OnPull();
                    _probe.Ref.Tell(GraphStageMessages.Pull.Instance);
                }
                catch (Exception e)
                {
                    _probe.Ref.Tell(new GraphStageMessages.StageFailure(GraphStageMessages.Pull.Instance, e));
                    throw;
                }
            }, onDownstreamFinish: () =>
            {
                try
                {
                    outHandler.OnDownstreamFinish();
                    _probe.Ref.Tell(GraphStageMessages.DownstreamFinish.Instance);

                }
                catch (Exception e)
                {
                    _probe.Ref.Tell(new GraphStageMessages.StageFailure(GraphStageMessages.DownstreamFinish.Instance, e));
                    throw;
                }
            });

            return logicAndMaterialized;
        }
    }
}