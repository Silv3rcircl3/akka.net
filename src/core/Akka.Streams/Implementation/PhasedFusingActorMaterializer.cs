//-----------------------------------------------------------------------
// <copyright file="PhasedFusingActorMaterializer.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Stage;
using Akka.Util;
using Reactive.Streams;
using Debug = System.Diagnostics.Debug;

namespace Akka.Streams.Implementation
{
    static class PhasedFusingActorMaterializer
    {
        private sealed class GraphPhase : IPhase<object>
        {
            public IPhaseIsland<object> Create(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer) => 
                new GraphStageIsland(settings, materializer);
        }

        private sealed class SinkPhase : IPhase<object>
        {
            public IPhaseIsland<object> Create(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer) =>
                new SinkModulePhase(materializer);
        }

        private sealed class SourcePhase : IPhase<object>
        {
            public IPhaseIsland<object> Create(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer) =>
                new SourceModulePhase(materializer);
        }

        internal static bool Debug = false;
        
        public static IPhase<object> DefaultPhase { get; } = new GraphPhase();

        public static ImmutableDictionary<IslandTag, IPhase<object>> DefaultPhases { get; } =
            new Dictionary<IslandTag, IPhase<object>>
            {
                { IslandTag.SinkModule, new SinkPhase() },
                { IslandTag.SourceModul, new SourcePhase() },
                { IslandTag.GraphStage, DefaultPhase }
            }.ToImmutableDictionary();

        public static ActorMaterializer Create(ActorMaterializerSettings settings, IActorRefFactory context)
        {
            var haveShutDown = new AtomicBoolean();
            var system = ActorSystemOf(context);
            var materializerSettings = ActorMaterializerSettings.Create(system);

            var supervisor = StreamSupervisor.Props(materializerSettings, haveShutDown)
                .WithDispatcher(materializerSettings.Dispatcher);

            // todo flownames EnumerableActorName.Create(system)
            return new PhasedFusingActorMaterializerImpl(system, materializerSettings, system.Dispatchers,
                context.ActorOf(supervisor, StreamSupervisor.NextName()), haveShutDown, null);
        }

        private static ActorSystem ActorSystemOf(IActorRefFactory context)
        {
            if (context is ExtendedActorSystem e)
                return e;
            if (context is IActorContext c)
                return c.System;
            if(context == null)
                throw  new ArgumentException("ActorRefFactory context must be defined", nameof(context));

            throw new ArgumentException($"ActorRefFactory context must be an ActorSystem or ActorContext, got [{context.GetType().Name}]");
        }
    }

    internal struct SegmentInfo
    {
        public int GlobalIslandOffset { get; }
        public int Length { get; }
        public int GlobalBaseOffset { get; }
        public int RelativeBaseOffset { get; }
        public IPhaseIsland<object> Phase { get; }

        public SegmentInfo(int globalIslandOffset, int length, int globalBaseOffset, int relativeBaseOffset, IPhaseIsland<object> phase)
        {
            GlobalIslandOffset = globalIslandOffset;
            Length = length;
            GlobalBaseOffset = globalBaseOffset;
            RelativeBaseOffset = relativeBaseOffset;
            Phase = phase;
        }

        public override string ToString() =>
            "| Segment\r\n" +
            $"|   GlobalIslandOffset = {GlobalIslandOffset}" +
            $"|   Length             = {Length}" +
            $"|   GlobalBaseOffset   = {GlobalBaseOffset}" +
            $"|   RelativeBaseOffset = {RelativeBaseOffset}" +
            $"|   Phase              = {Phase}";
    }

    internal struct ForwardWire
    {
        public int IslandGlobalOffset { get; }
        public OutPort From { get; }
        public int ToGlobalOffset { get; }
        public object OutStage { get; }
        public IPhaseIsland<object> Phase { get; }

        public ForwardWire(int islandGlobalOffset, OutPort from, int toGlobalOffset, object outStage, IPhaseIsland<object> phase)
        {
            IslandGlobalOffset = islandGlobalOffset;
            From = from;
            ToGlobalOffset = toGlobalOffset;
            OutStage = outStage;
            Phase = phase;
        }

        public override string ToString() =>
            $"ForwardWire(IslandId = {IslandGlobalOffset}, From = {From}, ToGlobal = {ToGlobalOffset}, Phase = {Phase}";
    }

    internal class IslandTracking
    {
        private int _currentGlobalOffset;
        private int _currentSegmentGlobalOffset;
        private int _currentIslandGlobalOffset;

        // The number of slots that belong to segments of other islands encountered so far, from the
        // beginning of the island
        private int _currentIslandSkippetSlots;

        private List<SegmentInfo> _segments;
        private List<ForwardWire> _forwardsWires;

        public IslandTracking(ImmutableDictionary<IslandTag, IPhase<object>> phases, ActorMaterializerSettings settings, IPhase<object> defaultPhase, PhasedFusingActorMaterializerImpl materializer)
        {
            Phases = phases;
            Settings = settings;
            CurrentPhase = defaultPhase.Create(settings, materializer);
            Materializer = materializer;
        }

        public ImmutableDictionary<IslandTag, IPhase<object>> Phases { get; }

        public ActorMaterializerSettings Settings { get; }

        public IPhaseIsland<object> CurrentPhase { get; private set; }

        public PhasedFusingActorMaterializerImpl Materializer { get; }

        public int CurrentOffset => _currentGlobalOffset;

        private int CompleteSegment()
        {
            var length = _currentGlobalOffset - _currentSegmentGlobalOffset;

            if (length > 0)
            {
                // We just finished a segment by entering an island.
                var previousSegment = new SegmentInfo(
                    globalIslandOffset: _currentIslandGlobalOffset,
                    length: _currentSegmentGlobalOffset,
                    globalBaseOffset: _currentSegmentGlobalOffset,
                    relativeBaseOffset: _currentSegmentGlobalOffset - _currentIslandGlobalOffset - _currentIslandSkippetSlots,
                    phase: CurrentPhase);

                // Segment tracking is by demand, we only allocate this list if it is used.
                // If there are no islands, then there is no need to track segments
                if(_segments == null)
                    _segments = new List<SegmentInfo>(8);
                _segments.Add(previousSegment);

                if(PhasedFusingActorMaterializer.Debug) Debug.Write($"Completed segment {previousSegment}");
            }
            else if (PhasedFusingActorMaterializer.Debug) Debug.Write("Skipped zero length segment");

            return length;
        }

        public ExitIsland EnterIsland(IslandTag tag)
        {
            CompleteSegment();
            var previousPhase = CurrentPhase;
            var previousIslandOffset = _currentIslandGlobalOffset;

            CurrentPhase = Phases[tag].Create(Settings, Materializer);

            if (PhasedFusingActorMaterializer.Debug)
                Debug.Write($"Entering island starting at offset = {_currentIslandGlobalOffset}, phase = {CurrentPhase}");

            // Resolve the phase to be used to materialize this island
            _currentIslandGlobalOffset = _currentGlobalOffset;

            // The base offset of this segment is the current global offset
            _currentSegmentGlobalOffset = _currentGlobalOffset;
            return new ExitIsland(previousIslandOffset, _currentIslandSkippetSlots, previousPhase);
        }

        public void ExitIsland(ExitIsland exitIsland)
        {
            var previousSegmentLength = CompleteSegment();

            // Closing previous island
            CurrentPhase.OnIslandReady();

            // We start a new segment
            _currentSegmentGlobalOffset = _currentGlobalOffset;

            // We restore data for the island
            _currentIslandGlobalOffset = exitIsland.IslandGlobalOffset;
            CurrentPhase = exitIsland.Phase;
            _currentIslandSkippetSlots = exitIsland.SkippedSlots + previousSegmentLength;

            if (PhasedFusingActorMaterializer.Debug)
                Debug.Write($"Exited to island starting at offset {_currentIslandGlobalOffset}, phase = {CurrentPhase}");
        }

        public void WireIn(InPort @in, object logic)
        {
            // The slot for this InPort always belong to the current segment, so resolving its local
            // offset/slot is simple
            var localInSlot = _currentGlobalOffset - _currentIslandGlobalOffset - _currentIslandSkippetSlots;
            if (PhasedFusingActorMaterializer.Debug)
                Debug.Write($"Wiring port {@in} inOffs absolute = {_currentGlobalOffset} local = {localInSlot}");

            // Assign the logic belonging to the current port to its calculated local slot in the island
            CurrentPhase.AssignPort(@in, localInSlot, logic);

            // Check if there was any forward wiring that has this offset/slot as its target
            // First try to find such wiring
            var foundWire = false;
            var forwardWire = new ForwardWire();
            if (_forwardsWires != null && _forwardsWires.Count > 0)
            {
                var i = 0;
                while (i < _forwardsWires.Count)
                {
                    forwardWire = _forwardsWires[i];
                    if (forwardWire.ToGlobalOffset == _currentGlobalOffset)
                    {
                        if (PhasedFusingActorMaterializer.Debug)
                            Debug.Write($"ther is a forward wire to this slot {forwardWire}");
                        _forwardsWires.RemoveAt(i);
                        foundWire = true;
                        i = int.MaxValue; // Exit the loop
                    }
                    else
                        i++;
                }
            }

            // If there is a forward wiring we need to resolve it
            if (foundWire)
            {
                // The forward wire ends up in the same island
                if (forwardWire.Phase == CurrentPhase)
                {
                    if(PhasedFusingActorMaterializer.Debug)
                        Debug.Write($"In-Island forward wiring from port {forwardWire.From} wired to local slot = {localInSlot}");

                    forwardWire.Phase.AssignPort(forwardWire.From, localInSlot, forwardWire.OutStage);
                }
                else
                {
                    if (PhasedFusingActorMaterializer.Debug)
                        Debug.Write($"Cross island forward wiring from port {forwardWire.From} wired to local slot = {localInSlot}");

                    var publisher = forwardWire.Phase.CreatePublisher(forwardWire.From, forwardWire.OutStage);
                    CurrentPhase.TakePublisher(localInSlot, publisher);
                }
            }

            _currentGlobalOffset++;
        }

        public void WireOut(OutPort @out, int absoluteOffset, object logic)
        {
            // TODO: forward wires
            if (PhasedFusingActorMaterializer.Debug)
                Debug.Write($"Wiring {@out} to absolute = {absoluteOffset}");

            // First check if we are wiring backwards. This is important since we can only do resolution for backward wires.
            // In other cases we need to record the forward wire and resolve it later once its target inSlot has been visited.

            if (absoluteOffset < _currentGlobalOffset)
            {
                if (PhasedFusingActorMaterializer.Debug)
                    Debug.Write("backward wiring");

                if (absoluteOffset >= _currentSegmentGlobalOffset)
                {
                    // Wiring is in the same segment, no complex lookup needed
                    var localInSlot = absoluteOffset - _currentIslandGlobalOffset - _currentIslandSkippetSlots;

                    if (PhasedFusingActorMaterializer.Debug)
                        Debug.Write($"in-segment wiring to local ({absoluteOffset} - {_currentIslandGlobalOffset} - {_currentIslandSkippetSlots}) = {localInSlot}");
                    CurrentPhase.AssignPort(@out, localInSlot, logic);
                }
                else
                {
                    // Wiring is cross-segment, but we don't know if it is cross-island or not yet
                    // We must find the segment to which this slot belongs first
                    var i = _segments.Count - 1;
                    var targetSegment = _segments[i];
                    // Skip segments that have a higher offset than our slot, until we find the containing segment
                    while (i > 0 && targetSegment.GlobalBaseOffset > absoluteOffset)
                    {
                        i--;
                        targetSegment = _segments[i];
                    }

                    // Independently of the target island the local slot for the target island is calculated the same:
                    // Independently of the target island the local slot for the target island is calculated the same:
                    //   - calculate the island relative offset by adding the island relative base offset of the segment
                    var distanceFromSegmentStart = absoluteOffset - targetSegment.GlobalBaseOffset;
                    var localInSlot = distanceFromSegmentStart + targetSegment.RelativeBaseOffset;

                    if (targetSegment.Phase == CurrentPhase)
                    {
                        if (PhasedFusingActorMaterializer.Debug)
                            Debug.Write($"cross-segment, in-island wiring to local slot {localInSlot}");

                        CurrentPhase.AssignPort(@out, localInSlot, logic);
                    }
                    else
                    {
                        if (PhasedFusingActorMaterializer.Debug)
                            Debug.Write($"cross-island wiring to local slot {localInSlot} in target island");

                        var publisher = CurrentPhase.CreatePublisher(@out, logic);
                        targetSegment.Phase.TakePublisher(localInSlot, publisher);
                    }
                }
            }
            else
            {
                // We need to record the forward wiring so we can resolve it later

                // The forward wire tracking data structure is only allocated when needed. Many graphs have no forward wires
                // even though it might have islands.
                if(_forwardsWires == null)
                    _forwardsWires = new List<ForwardWire>(8);

                var forwardWire = new ForwardWire(
                    islandGlobalOffset: _currentIslandGlobalOffset,
                    from: @out,
                    toGlobalOffset: absoluteOffset,
                    outStage: logic,
                    phase: CurrentPhase);

                if (PhasedFusingActorMaterializer.Debug)
                    Debug.Write($"wiring is forward, recording {forwardWire}");

                _forwardsWires.Add(forwardWire);
            }
        }
    }

    public class PhasedFusingActorMaterializerImpl : ExtendedActorMaterializer
    {
        private readonly Dispatchers _dispatchers;
        private readonly AtomicBoolean _haveShutDown;
        private readonly IEnumerable<string> _flowNames;
        private readonly Attributes _defaultInitialAttributes;

        public PhasedFusingActorMaterializerImpl(ActorSystem system, ActorMaterializerSettings settings,
            Dispatchers dispatchers, IActorRef supervisor, AtomicBoolean haveShutDown, EnumerableActorName flowNames)
        {
            System = system;
            _dispatchers = dispatchers;
            Supervisor = supervisor;
            _haveShutDown = haveShutDown;
            _flowNames = flowNames;

            var buffer = Attributes.CreateInputBuffer(settings.InitialInputBufferSize, settings.MaxInputBufferSize);
            var dispatcher = ActorAttributes.CreateDispatcher(settings.Dispatcher);
            var supervision = ActorAttributes.CreateSupervisionStrategy(settings.SupervisionDecider);
            var attributes = buffer.AttributeList.Concat(dispatcher.AttributeList).Concat(supervision.AttributeList).ToArray();
            _defaultInitialAttributes = new Attributes(attributes);

            Settings = settings;
            var logger = Logging.GetLogger(system, this);

            if(settings.IsFuzzingMode && !system.Settings.Config.HasPath("akka.stream.secret-test-fuzzing-warning-disable"))
                logger.Warning("Fuzzing mode is enabled on this system. If you see this warning on your production system then " +
                               "set akka.stream.materializer.debug.fuzzing-mode to off.");

            Logger = logger;
        }
        
        public override ActorMaterializerSettings Settings { get; }

        public override bool IsShutdown => _haveShutDown.Value;

        public override ActorMaterializerSettings EffectiveSettings(Attributes attributes) =>
            attributes.AttributeList.Aggregate(Settings, (settings, attribute) =>
            {
                if (attribute is Attributes.InputBuffer buffer)
                    return settings.WithInputBuffer(buffer.Initial, buffer.Max);
                if (attribute is ActorAttributes.Dispatcher dispatcher)
                    return settings.WithDispatcher(dispatcher.Name);
                if (attribute is ActorAttributes.SupervisionStrategy strategy)
                    return settings.WithSupervisionStrategy(strategy.Decider);

                return settings;
            });

        public override void Shutdown()
        {
            if(_haveShutDown.CompareAndSet(false, true))
                Supervisor.Tell(PoisonPill.Instance);
        }

        public override ILoggingAdapter MakeLogger(object logSource) => Logging.GetLogger(System, logSource);

        public override MessageDispatcher ExecutionContext
        {
            get
            {
                if (_executionContext == null)
                {
                    var dispatcherName = Settings.Dispatcher == Deploy.NoDispatcherGiven
                        ? Dispatchers.DefaultDispatcherId
                        : Settings.Dispatcher;
                    _executionContext = _dispatchers.Lookup(dispatcherName);
                }

                return _executionContext;
            }
        }

        private MessageDispatcher _executionContext;

        public override ActorSystem System { get; }

        public override ILoggingAdapter Logger { get; }

        public override IActorRef Supervisor { get; }

        public override IMaterializer WithNamePrefix(string namePrefix)
        {
            return null;
            // TODO FlowNames
        }

        public override TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable) =>
            Materialize(runnable, null, _defaultInitialAttributes);

        public override TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Attributes initialAttributes) =>
            Materialize(runnable, null, initialAttributes);

        public override TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Func<ActorGraphInterpreter.GraphInterpreterShell, IActorRef> subFlowFuser) =>
            Materialize(runnable, subFlowFuser, _defaultInitialAttributes);

        public override TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Func<ActorGraphInterpreter.GraphInterpreterShell, IActorRef> subFlowFuser, Attributes initialAttributes)
        {
            return Materialize(runnable, subFlowFuser, initialAttributes, 
                PhasedFusingActorMaterializer.DefaultPhase,
                PhasedFusingActorMaterializer.DefaultPhases);
        }

        public TMat Materialize<TMat>(IGraph<ClosedShape, TMat> graph,
            Func<ActorGraphInterpreter.GraphInterpreterShell, IActorRef> subFlowFuser, Attributes initialAttributes,
            IPhase<object> defaultPhase, ImmutableDictionary<IslandTag, IPhase<object>> phases)
        {
            var islandTracking = new IslandTracking(phases, Settings, defaultPhase, this);

            // todo TraversalBuilder
            //var current = graph.traversalBuilder
            return default(TMat);
        }

        public override ICancelable ScheduleOnce(TimeSpan delay, Action action) =>
            System.Scheduler.Advanced.ScheduleOnceCancelable(delay, action);

        public override ICancelable ScheduleRepeatedly(TimeSpan initialDelay, TimeSpan interval, Action action) =>
            System.Scheduler.Advanced.ScheduleRepeatedlyCancelable(initialDelay, interval, action);

        // TODO move to extendactormaterializer
        internal void ActorOf(Props props, string v, string dispatcher)
        {
            throw new NotImplementedException();
        }
    }

    public interface IPhase<T>
    {
        IPhaseIsland<T> Create(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer);
    }

    public enum IslandTag
    {
        SourceModul,
        GraphStage,
        SinkModule
    }

    public interface IPhaseIsland<T>
    {
        string Name { get; }

        Tuple<T, object> MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes);

        void AssignPort(InPort @in, int slot, T logic);

        void AssignPort(OutPort @out, int slot, T logic);

        IPublisher<object> CreatePublisher(OutPort @out, T logic);

        void TakePublisher(int slot, IPublisher<object> publisher);

        void OnIslandReady();
    }

    public sealed class GraphStageIsland : IPhaseIsland<GraphStageLogic>, IPhaseIsland<object>
    {
        private readonly Random _random = new Random();
        private readonly ActorMaterializerSettings _settings;
        private readonly PhasedFusingActorMaterializerImpl _materializer;

        // TODO: remove these
        private readonly List<GraphStageLogic> _logics = new List<GraphStageLogic>(64);
        // TODO: Resize
        private readonly GraphInterpreter.Connection[] _connections = new GraphInterpreter.Connection[64];
        private int _maxConnections;
        private readonly List<GraphInterpreter.Connection> _outConnections = new List<GraphInterpreter.Connection>();

        public GraphStageIsland(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer)
        {
            _settings = settings;
            _materializer = materializer;

            Shell = new ActorGraphInterpreter.GraphInterpreterShell(null, null, _settings, _materializer);
        }

        // todo remove assembly from graphinterpreter shell
        public ActorGraphInterpreter.GraphInterpreterShell Shell { get; }

        public string Name { get; } = "Fusing GraphStages phase";

        Tuple<object, object> IPhaseIsland<object>.MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes)
        {
            var t = MaterializeAtomic(module, attributes);
            return new Tuple<object, object>(t.Item1, t.Item2);
        }

        void IPhaseIsland<object>.AssignPort(InPort @in, int slot, object logic) =>
            AssignPort(@in, slot, (GraphStageLogic)logic);

        void IPhaseIsland<object>.AssignPort(OutPort @out, int slot, object logic) =>
            AssignPort(@out, slot, (GraphStageLogic)logic);

        IPublisher<object> IPhaseIsland<object>.CreatePublisher(OutPort @out, object logic) =>
            CreatePublisher(@out, (GraphStageLogic)logic);

        public Tuple<GraphStageLogic, object> MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes)
        {
            // TODO: bail on unknown types
            var stageModule = (GraphStageModule)module;
            var matAndLogic = stageModule.Stage.CreateLogicAndMaterializedValue(attributes);
            var logic = matAndLogic.Logic;
            _logics.Add(logic);
            logic.StageId = _logics.Count - 1;
            return Tuple.Create(logic, matAndLogic.MaterializedValue);
        }

        public GraphInterpreter.Connection Connection(int slot)
        {
            _maxConnections = Math.Max(slot, _maxConnections);
            var c = _connections[slot];
            if (c != null)
                return c;

            var c2 = new GraphInterpreter.Connection(0, 0, null, 0, null, null, null);
            _connections[slot] = c2;
            return c2;
        }

        public GraphInterpreter.Connection OutConnection()
        {
            var connection = new GraphInterpreter.Connection(0, 0, null, 0, null, null, null);
            _outConnections.Add(connection);
            return connection;
        }

        public void AssignPort(InPort @in, int slot, GraphStageLogic logic)
        {
            var connection = Connection(slot);
            connection.InOwner = logic;
            connection.Id = slot;
            connection.InOwnerId = logic.StageId;
            connection.InHandler = logic.Handlers[@in.Id] as IInHandler;
            logic.PortToConn[@in.Id] = connection;
        }

        public void AssignPort(OutPort @out, int slot, GraphStageLogic logic)
        {
            var connection = Connection(slot);
            connection.OutOwner = logic;
            connection.Id = slot;
            connection.OutOwnerId = logic.StageId;
            connection.OutHandler = logic.Handlers[@out.Id] as IOutHandler;
            logic.PortToConn[logic.InCount + @out.Id] = connection;
        }

        public IPublisher<object> CreatePublisher(OutPort @out, GraphStageLogic logic)
        {
            // todo remove object type parameter
            var boundary = new ActorGraphInterpreter.ActorOutputBoundary<object>(Shell, @out.ToString());
            _logics.Add(boundary);
            boundary.StageId = _logics.Count - 1;

            var connection = OutConnection();
            boundary.PortToConn[boundary.In.Id] = connection;
            connection.InHandler = boundary.Handlers[0] as IInHandler;
            connection.InOwner = boundary;
            connection.InOwnerId = boundary.StageId;

            connection.OutOwner = logic;
            connection.Id = -1; // Will be filled later
            connection.OutOwnerId = logic.StageId;
            connection.OutHandler = logic.Handlers[logic.InCount + @out.Id] as IOutHandler;
            logic.PortToConn[logic.InCount + @out.Id] = connection;

            return boundary.Publisher;
        }

        public void TakePublisher(int slot, IPublisher<object> publisher)
        {
            // TODO: proper input buffer sizes from attributes
            var connection = Connection(slot);
            // TODO: proper input port debug string (currently prints the stage)
            var boundary =
                new ActorGraphInterpreter.BatchingActorInputBoundary<object>(16, Shell, publisher,
                    connection.InOwner.ToString());
            _logics.Add(boundary);
            boundary.StageId = _logics.Count - 1;

            boundary.PortToConn[boundary.Out.Id + boundary.InCount] = connection;
            connection.OutHandler = boundary.Handlers[0] as IOutHandler;
            connection.OutOwner = boundary;
            connection.OutOwnerId = boundary.StageId;
        }

        public void OnIslandReady()
        {
            var totalConnections = _maxConnections + _outConnections.Count + 1;
            var finalConnections = new GraphInterpreter.Connection[totalConnections];
            Array.Copy(_connections, finalConnections, totalConnections);
            
            var outIndex = 0;
            for (var i = _maxConnections + 1; i < totalConnections; i++)
            {
                var connection = _outConnections[outIndex];
                finalConnections[i] = connection;
                connection.Id = i;
                outIndex++;
            }

            Shell.Connections = finalConnections;
            Shell.Logics = _logics.ToArray();

            // TODO: Subfusing
            var props = ActorGraphInterpreter.Props(Shell);

            // TODO: actor names
            _materializer.ActorOf(props, "fused" + _random.Next(), _settings.Dispatcher);
        }

        public override string ToString() => "GraphStagePhase";
    }

    public sealed class SourceModulePhase : IPhaseIsland<IPublisher<object>>, IPhaseIsland<object>
    {
        private readonly PhasedFusingActorMaterializerImpl _materializer;

        public SourceModulePhase(PhasedFusingActorMaterializerImpl materializer) => _materializer = materializer;

        public string Name { get; } = "SourceModule phase";

        Tuple<object, object> IPhaseIsland<object>.MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes)
        {
            var t = MaterializeAtomic(module, attributes);
            return new Tuple<object, object>(t.Item1, t.Item2);
        }

        void IPhaseIsland<object>.AssignPort(InPort @in, int slot, object logic)
        {
        }

        void IPhaseIsland<object>.AssignPort(OutPort @out, int slot, object logic)
        {
        }

        IPublisher<object> IPhaseIsland<object>.CreatePublisher(OutPort @out, object logic) => (IPublisher<object>)logic;

        public Tuple<IPublisher<object>, object> MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes)
        {
            // TODO: proper stage name
            var source = (SourceModule<object, object>)module;
            var context = new MaterializationContext(_materializer, attributes, "stageName");
            var publisher = source.Create(context, out var logic);
            return Tuple.Create(publisher, logic);
        }

        public void AssignPort(InPort @in, int slot, IPublisher<object> logic)
        {
        }

        public void AssignPort(OutPort @out, int slot, IPublisher<object> logic)
        {
        }

        public IPublisher<object> CreatePublisher(OutPort @out, IPublisher<object> logic) => logic;

        public void TakePublisher(int slot, IPublisher<object> publisher) => throw new InvalidOperationException("A Source cannot take a IPublisher");

        public void OnIslandReady()
        {
        }
    }

    public sealed class SinkModulePhase : IPhaseIsland<object>
    {
        private readonly PhasedFusingActorMaterializerImpl _materializer;
        private object _subscriberOrVirtualPublisher;

        public SinkModulePhase(PhasedFusingActorMaterializerImpl materializer) => _materializer = materializer;

        public string Name { get; } = "SinkModule phase";

        public Tuple<object, object> MaterializeAtomic(IAtomicModule<Shape> module, Attributes attributes)
        {
            // TODO: proper stage name
            var sink = (SinkModule<object, object>) module;
            var context = new MaterializationContext(_materializer, attributes, "stageName");
            _subscriberOrVirtualPublisher = sink.Create(context, out var logic);
            return Tuple.Create(_subscriberOrVirtualPublisher, logic);
        }

        public void AssignPort(InPort @in, int slot, object logic)
        {
        }

        public void AssignPort(OutPort @out, int slot, object logic)
        {
        }

        public IPublisher<object> CreatePublisher(OutPort @out, object logic) => throw new InvalidOperationException("A Sink cannot create a IPublisher");

        public void TakePublisher(int slot, IPublisher<object> publisher)
        {
            if(_subscriberOrVirtualPublisher is VirtualPublisher<object> v)
                v.RegisterPublisher(publisher);
            else if (_subscriberOrVirtualPublisher is ISubscriber<object> s)
                publisher.Subscribe(s);
        }

        public void OnIslandReady()
        {
        }
    }
}
