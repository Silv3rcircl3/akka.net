//-----------------------------------------------------------------------
// <copyright file="PhasedFusingActorMaterializer.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Streams.Implementation
{
    static class PhasedFusingActorMaterializer
    {
        internal static bool Debug = false;
    }

    internal struct SegmentInfo
    {
        public int GlobalIslandOffset { get; }
        public int Length { get; }
        public int GlobalBaseOffset { get; }
        public int RelativeBaseOffset { get; }
        public IPhaseIsland Phase { get; }

        public SegmentInfo(int globalIslandOffset, int length, int globalBaseOffset, int relativeBaseOffset, IPhaseIsland phase)
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
        public IPhaseIsland Phase { get; }

        public ForwardWire(int islandGlobalOffset, OutPort from, int toGlobalOffset, object outStage, IPhaseIsland phase)
        {
            IslandGlobalOffset = islandGlobalOffset;
            From = from;
            ToGlobalOffset = toGlobalOffset;
            OutStage = outStage;
            Phase = phase;
        }

        public override string ToString() =>
            $"ForwardWire(IslandId = {IslandGlobalOffset}, From = {From}, ToGlobal = {ToGlobalOffset}, Phase = {Phase}";

        public object CreatePublisher(OutPort @from, object forwardWireOutStage)
        {
            throw new NotImplementedException();
        }
    }

    internal class IslandTracking
    {
        private int _currentGlobalOffset = 0;
        private int _currentSegmentGlobalOffset = 0;
        private int _currentIslandGlobalOffset = 0;

        // The number of slots that belong to segments of other islands encountered so far, from the
        // beginning of the island
        private int _currentIslandSkippetSlots = 0;

        private List<SegmentInfo> _segments;
        private List<ForwardWire> forwardsWires;

        public IslandTracking(Dictionary<IIslandTag, IPhase> phases, ActorMaterializerSettings settings, IPhase defaultPhase, PhasedFusingActorMaterializerImpl materializer)
        {
            Phases = phases;
            Settings = settings;
            CurrentPhase = defaultPhase.Create(settings, materializer);
            Materializer = materializer;
        }

        public Dictionary<IIslandTag, IPhase> Phases { get; }

        public ActorMaterializerSettings Settings { get; }

        public IPhaseIsland CurrentPhase { get; private set; }

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

                if(PhasedFusingActorMaterializer.Debug) Debug.Print($"Completed segment {previousSegment}");


            }
            else if (PhasedFusingActorMaterializer.Debug) Debug.Print("Skipped zero length segment");

            return length;
        }

        public ExitIsland EnterIsland(IIslandTag tag)
        {
            CompleteSegment();
            var previousPhase = CurrentPhase;
            var previousIslandOffset = _currentIslandGlobalOffset;

            CurrentPhase = Phases[tag].Create(Settings, Materializer);

            if (PhasedFusingActorMaterializer.Debug)
                Debug.Print($"Entering island starting at offset = {_currentIslandGlobalOffset}, phase = {CurrentPhase}");

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
                Debug.Print($"Exited to island starting at offset {_currentIslandGlobalOffset}, phase = {CurrentPhase}");
        }

        public void WireIn(InPort @in, object logic)
        {
            // The slot for this InPort always belong to the current segment, so resolving its local
            // offset/slot is simple
            var localInSlot = _currentGlobalOffset - _currentIslandGlobalOffset - _currentIslandSkippetSlots;
            if (PhasedFusingActorMaterializer.Debug)
                Debug.Print($"Wiring port {@in} inOffs absolute = {_currentGlobalOffset} local = {localInSlot}");

            // Assign the logic belonging to the current port to its calculated local slot in the island
            CurrentPhase.AssignPort(@in, localInSlot, logic);

            // Check if there was any forward wiring that has this offset/slot as its target
            // First try to find such wiring
            var foundWire = false;
            var forwardWire = new ForwardWire();
            if (forwardsWires != null && forwardsWires.Count > 0)
            {
                var i = 0;
                while (i < forwardsWires.Count)
                {
                    forwardWire = forwardsWires[i];
                    if (forwardWire.ToGlobalOffset == _currentGlobalOffset)
                    {
                        if (PhasedFusingActorMaterializer.Debug)
                            Debug.Print($"ther is a forward wire to this slot {forwardWire}");
                        forwardsWires.RemoveAt(i);
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
                        Debug.Print($"In-Island forward wiring from port {forwardWire.From} wired to local slot = {localInSlot}");

                    forwardWire.Phase.AssignPort(forwardWire.From, localInSlot, forwardWire.OutStage);
                }
                else
                {
                    if (PhasedFusingActorMaterializer.Debug)
                        Debug.Print($"Cross island forward wiring from port {forwardWire.From} wired to local slot = {localInSlot}");

                    var publisher = forwardWire.CreatePublisher(forwardWire.From, forwardWire.OutStage);
                    CurrentPhase.TakePublisher(localInSlot, publisher);
                }
            }

            _currentGlobalOffset++;
        }
    }

    // TODO move to traversel builder file
    // Never embedded into actual traversal, used as a marker in AbsoluteTraversal
    internal class ExitIsland
    {
        public int IslandGlobalOffset { get; }
        public int SkippedSlots { get; }
        public IPhaseIsland Phase { get; }

        public ExitIsland(int islandGlobalOffset, int skippedSlots, IPhaseIsland phase)
        {
            IslandGlobalOffset = islandGlobalOffset;
            SkippedSlots = skippedSlots;
            Phase = phase;
        }
    }

    internal class PhasedFusingActorMaterializerImpl
    {
    }

    internal interface IPhase
    {
        IPhaseIsland Create(ActorMaterializerSettings settings, PhasedFusingActorMaterializerImpl materializer);
    }

    internal interface IIslandTag
    {
    }

    internal interface IPhaseIsland
    {
        void OnIslandReady();
        void AssignPort(InPort @in, int localInSlot, object logic);
        void AssignPort(OutPort @out, int localInSlot, object forwardWireOutStage);
        void TakePublisher(int localInSlot, object publisher);
    }
}
