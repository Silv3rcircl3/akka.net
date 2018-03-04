//-----------------------------------------------------------------------
// <copyright file="TraversalBuilder.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------


using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using Akka.Pattern;
using Akka.Streams.Dsl;
using Akka.Streams.Util;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// Graphs to be materialized are defined by their traversal. There is no explicit graph information tracked, instead
    /// a sequence of steps required to "reconstruct" the graph.
    /// 
    /// <para/>
    /// 
    /// "Reconstructing" a graph here has a very clear-cut definition: assign a gapless range of integers from
    /// 0..connectionCount to inputs and outputs of modules, so that those that are wired together receive the same
    /// number (and those which are not receive different numbers). This feature can be used to
    ///  - materialize a graph, using the slots as indices to an array of Publishers/Subscribers that need to be wired together
    ///  - fuse a graph, using the slots to construct a <see cref="GraphAssembly"/> which uses a similar layout
    ///  - create a DOT formatted output for visualization
    ///  - convert the graph to another data structure
    /// 
    /// <para/>
    /// 
    /// The Traversal is designed to be position independent so that multiple traversals can be composed relatively
    /// simply. This particular feature also avoids issues with multiply imported modules where the identity must
    /// be encoded somehow. The two imports don't need any special treatment as they are at different positions in
    /// the traversal. See <see cref="MaterializeAtomic{TShape}"/> for more details.
    /// </summary>
    public abstract class Traversal
    {
        /// <summary>
        /// Concatenates two traversals building a new Traversal which traverses both.
        /// </summary>
        public virtual Traversal Concat(Traversal that) => NormalizeConcat(this, that);

        private static Traversal NormalizeConcat(Traversal first, Traversal second)
        {
            if (second == EmptyTraversal.Instance) return first;

            if (first == PushNotUsed.Instance)
            {
                // No need to push NotUsed and Pop it immediately
                if (second == Pop.Instance) return EmptyTraversal.Instance;
                if (second is Concat c && c.First == Pop.Instance) return c.Next;
                return new Concat(PushNotUsed.Instance, second);
            }

            // Limit the tree by rotations
            if (first is Concat concat)
            {
                // Note that we DON'T use firstfirst.concat(firstsecond.concat(second)) here,
                // although that would fully linearize the tree.
                // The reason is to simply avoid going n^2. The rotation below is of constant time and good enough.
                return new Concat(concat.First, new Concat(concat.Next, second));
            }

            return new Concat(first, second);
        }

        public virtual Traversal RewireFirstTo(int relativeOffset) => null;
    }

    /// <summary>
    /// A Traversal that consists of two traversals. The linked traversals must be traversed in first, next order.
    /// </summary>
    sealed class Concat : Traversal
    {
        public Traversal First { get; }
        public Traversal Next { get; }

        public Concat(Traversal first, Traversal next)
        {
            First = first;
            Next = next;
        }

        public override Traversal RewireFirstTo(int relativeOffset)
        {
            var firstResult = First.RewireFirstTo(relativeOffset);
            return firstResult != null
                ? firstResult.Concat(Next)
                : First.Concat(Next.RewireFirstTo(relativeOffset));
        }
    }

    /// <summary>
    /// Arriving at this step means that an atomic module needs to be materialized (or any other activity which
    /// assigns something to wired output-input port pairs).
    /// 
    /// <para/>
    /// 
    /// The traversing party must assign port numbers in the following way:
    ///  - input ports are implicitly assigned to numbers. Every module's input ports are assigned to consecutive numbers
    ///    according to their order in the shape. In other words, the materializer only needs to keep a counter and
    ///    increment it for every visited input port.
    ///  - the assigned number of the first input port for every module should be saved while materializing the module.
    ///    every output port should be assigned to (base + outToSlots(out.id)) where base is the number of the first
    ///    input port of the module (or the last unused input number if it has no input ports) and outToSlots is the
    ///    array provided by the traversal step.
    /// 
    /// <para/>
    /// 
    /// Since the above two rules always require local only computations (except a counter) this achieves
    /// positional independence of materializations.
    /// 
    /// <para/>
    /// TODO 
    /// See the `TraversalTestUtils` class and the `testMaterialize` method for a simple example.
    /// </summary>
    sealed class MaterializeAtomic<TShape> : Traversal where TShape : Shape
    {
        public IAtomicModule<TShape> Module { get; }

        public int[] OutToSlots { get; }

        public MaterializeAtomic(IAtomicModule<TShape> module, int[] outToSlots)
        {
            Module = module;
            OutToSlots = outToSlots;
        }

        public override Traversal RewireFirstTo(int relativeOffset) =>
            new MaterializeAtomic<TShape>(Module, new int[relativeOffset]);
    }

    /// <summary>
    /// Traversal with no steps.
    /// </summary>
    sealed class EmptyTraversal : Traversal
    {
        public static EmptyTraversal Instance { get; } = new EmptyTraversal();

        private EmptyTraversal() { }

        public override Traversal Concat(Traversal that) => that;
    }
    
    class MaterializedValueOp : Traversal { }

    sealed class PushNotUsed : MaterializedValueOp
    {
        public static PushNotUsed Instance { get; } = new PushNotUsed();

        private PushNotUsed() { }
    }

    sealed class Pop : MaterializedValueOp
    {
        public static Pop Instance { get; } = new Pop();

        private Pop() { }
    }

    sealed class Transform : MaterializedValueOp
    {
        public Func<object, object> Mapper { get; }

        public Transform(Func<object, object> mapper) => Mapper = mapper;
    }

    sealed class Compose : MaterializedValueOp
    {
        public Func<object, object, object> Composer { get; }

        public Compose(Func<object, object, object> composer) => Composer = composer;
    }

    sealed class PushAttributes : Traversal
    {
        public Attributes Attributes { get; }

        public PushAttributes(Attributes attributes) => Attributes = attributes;
    }

    sealed class PopAttributes : Traversal
    {
        public static PopAttributes Instance { get; } = new PopAttributes();

        private PopAttributes() { }
    }

    sealed class EnterIsland : Traversal
    {
        public IslandTag IslandTag { get; }
        public Traversal Island { get; }

        public EnterIsland(IslandTag islandTag, Traversal island)
        {
            IslandTag = islandTag;
            Island = island;
        }

        public override Traversal RewireFirstTo(int relativeOffset) =>
            new EnterIsland(IslandTag, Island.RewireFirstTo(relativeOffset));
    }

    // Never embedded into actual traversal, used as a marker in AbsoluteTraversal
    sealed class ExitIsland : Traversal
    {
        public int IslandGlobalOffset { get; }
        public int SkippedSlots { get; }
        public IPhaseIsland<object> Phase { get; }

        public ExitIsland(int islandGlobalOffset, int skippedSlots, IPhaseIsland<object> phase)
        {
            IslandGlobalOffset = islandGlobalOffset;
            SkippedSlots = skippedSlots;
            Phase = phase;
        }
    }

    static class TraversalBuilder
    {
        private static readonly CompletedTraversalBuilder CachedEmptyCompleted =
            new CompletedTraversalBuilder(PushNotUsed.Instance, 0, ImmutableDictionary<InPort, int>.Empty, Attributes.None);

        /// <summary>
        /// Assign ports their id, which is their position inside the Shape. This is used both by the GraphInterpreter
        /// and the layout system here.
        /// </summary>
        public static void InitShape(Shape shape)
        {
            // Initialize port IDs
            for (int i = 0; i < shape.Inlets.Length; i++)
                shape.Inlets[i].Id = i;

            for (int i = 0; i < shape.Outlets.Length; i++)
                shape.Outlets[i].Id = i;
        }

        public static CompletedTraversalBuilder Empty(Attributes attributes = null)
        {
            if (attributes == null || attributes == Attributes.None)
                return CachedEmptyCompleted;

            return new CompletedTraversalBuilder(PushNotUsed.Instance, 0, ImmutableDictionary<InPort, int>.Empty, attributes);
        }

        public static ITraversalBuilder Atomic<TShape>(IAtomicModule<TShape> module, Attributes attributes = null) where TShape : Shape
        {
            InitShape(module.Shape);

            if (module.Shape.Outlets.IsEmpty)
            {
                return new CompletedTraversalBuilder(
                    traversalSoFar: new MaterializeAtomic<TShape>(module, new int[module.Shape.Outlets.Length]),
                    inSlots: module.Shape.Inlets.Length,
                    inToOffset: module.Shape.Inlets.ToImmutableDictionary(i => (InPort)i, i => i.Id),
                    attributes: attributes);
            }

            return new AtomicTraversalBuilder<TShape>(
                module,
                new int[module.Shape.Outlets.Length],
                module.Shape.Outlets.Length,
                attributes);
        }

        public static void PrintTraversal(Traversal t, int ident = 0)
        {
            var current = t;
            var identString = Enumerable.Range(0, ident).Select(_ => " | ").Aggregate("", (acc, s) => acc + s);

            void PrintIdent(string s) => Debug.WriteLine(identString + s);

            while (current != EmptyTraversal.Instance)
            {
                Traversal nextStep = EmptyTraversal.Instance;

                switch (current)
                {
                    case PushNotUsed _:
                        PrintIdent("push NotUsed");
                        break;
                    case Pop _:
                        PrintIdent("pop mat");
                        break;
                    case Transform _:
                        PrintIdent("transform mat");
                        break;
                    case Compose _:
                        PrintIdent("compose mat");
                        break;
                    case PushAttributes attributes:
                        PrintIdent("push attr " + attributes);
                        break;
                    case PopAttributes _:
                        PrintIdent("pop attr");
                        break;
                    case EnterIsland island:
                        PrintIdent("enter island " + island.IslandTag);
                        PrintTraversal(island.Island);
                        break;
                    //Todo type parameter
                    //case MaterializeAtomic atomic:
                    //    PrintIdent("materialize " + atomic.Module + " [" + string.Join(", ", atomic.OutToSlots) + "]");
                    //    break;
                    case Concat concat:
                        PrintTraversal(concat.First, ident + 1);
                        nextStep = concat.Next;
                        break;
                }

                current = nextStep;
            }
        }

        public static int PrintWiring(Traversal t, int baseSlot = 0)
        {
            var current = t;
            var slot = baseSlot;

            while (current != EmptyTraversal.Instance)
            {
                Traversal nextStep = EmptyTraversal.Instance;
                
                // todo generic types
                //if (current is MaterializeAtomic atomic)
                //{
                //    Debug.WriteLine("materialize " + atomic.Module);
                //    var @base = slot;

                //    foreach (var i in atomic.Module.Shape.Inlets)
                //    {
                //        Debug.WriteLine($"  wiring {i} to {slot}");
                //        slot++;
                //    }

                //    foreach (var o in atomic.Module.Shape.Outlets)
                //        Debug.WriteLine($"  wiring {o} to ${@base + atomic.OutToSlots[o.Id]}");
                //}
                //else if (current is Concat c)
                //{
                //    slot = PrintWiring(c.First, slot);
                //    nextStep = c.Next;
                //}
                //else if (current is EnterIsland e)
                //    nextStep = e.Island;

                current = nextStep;
            }

            return slot;
        }
    }

    /// <summary>
    /// A builder for a Traversal. The purpose of subclasses of this trait is to eventually build a Traversal that
    /// describes the graph. Depending on whether the graph is linear or generic different approaches can be used but
    /// they still result in a Traversal.
    /// 
    /// The resulting Traversal can be accessed via the <see cref="Traversal"/> method once the graph is completed (all ports are
    /// wired). The Traversal may be accessed earlier, depending on the type of the builder and certain conditions.
    /// 
    /// See <see cref="CompositeTraversalBuilder"/> and <see cref="LinearTraversalBuilder"/>.
    /// </summary>
    public interface ITraversalBuilder
    {
        /// <summary>
        /// Adds a module to the builder. It is possible to add a module with a different Shape (import), in this
        /// case the ports of the shape MUST have their `mappedTo` field pointing to the original ports. The act of being
        /// imported will not be reflected in the final Traversal, the Shape is only used by the builder to disambiguate
        /// between multiple imported instances of the same module.
        /// 
        /// See append in the <see cref="LinearTraversalBuilder"/> for a more efficient alternative for linear graphs.
        /// </summary>
        ITraversalBuilder Add<TMat, TMat1, TMat2>(ITraversalBuilder submodule, Shape shape, Func<TMat, TMat1, TMat2> combineMaterialized);

        /// <summary>
        /// Maps the materialized value produced by the module built-up so far with the provided function, providing a new
        /// TraversalBuilder returning the mapped materialized value.
        /// </summary>
        ITraversalBuilder TransformMataterialized<TMat, TMat1>(Func<TMat, TMat1> mapper);

        ITraversalBuilder SetAttributes(Attributes attributes);

        Attributes Attributes { get; }

        /// <summary>
        /// Connects two unwired ports in the graph. For imported modules, use the ports of their "import shape". These
        /// ports must have their `mappedTo` field set and point to the original ports.
        /// 
        /// See append in the <see cref="LinearTraversalBuilder"/> for a more efficient alternative for linear graphs.
        /// </summary>
        ITraversalBuilder Wire(OutPort @out, InPort @in);

        /// <summary>
        /// Returns the base offset (the first number an input port would receive if there is any) of the module to which
        /// the port belongs *relative to this builder*. This is used to calculate the relative offset of output port mappings
        /// (<see cref="MaterializeAtomic{TShape}"/>).
        /// 
        /// This method only guarantees to return the offset of modules for output ports that have not been wired.
        /// </summary>
        int OffsetOfModule(OutPort @out);

        /// <summary>
        /// Returns whether the given output port has been wired in the graph or not.
        /// </summary>
        bool IsUnwired(OutPort @out);

        /// <summary>
        /// Returns whether the given inport port has been wired in the graph or not.
        /// </summary>
        bool IsUnwired(InPort @in);

        /// <summary>
        /// Returns the number assigned to a certain input port *relative* to this module.
        /// This method only guarantees to return the offset of input ports that have not been wired.
        /// </summary>
        int OffsetOf(InPort @in);

        /// <summary>
        /// Finish the wiring of an output port to an input port by assigning the relative slot for the output port.
        /// See <see cref="MaterializeAtomic{TShape}"/> for details of the resolution process)
        /// </summary>
        ITraversalBuilder Assign(OutPort @out, int relativeSlot);

        /// <summary>
        /// Returns true if the Traversal is available. Not all builders are able to build up the Traversal incrementally.
        /// Generally a traversal is complete if there are no unwired output ports.
        /// </summary>
        bool IsTraversalComplete { get; }

        /// <summary>
        /// The total number of input ports encountered so far. Gives the first slot to which a new input port can be
        /// assigned (if a new module is added).
        /// </summary>
        int InSlots { get; }

        /// <summary>
        /// Returns the Traversal if ready for this (sub)graph.
        /// </summary>
        Traversal Traversal { get; }

        /// <summary>
        /// The number of output ports that have not been wired.
        /// </summary>
        int UnwiredOuts { get; }

        /// <summary>
        /// Wraps the builder in an island that can be materialized differently, using async boundaries to bridge
        /// between islands.
        /// </summary>
        ITraversalBuilder MakeIsland(IslandTag islandTag);
    }

    /// <summary>
    /// Returned by <see cref="CompositeTraversalBuilder"/> once all output ports of a subgraph has been wired.
    /// </summary>
    internal sealed class CompletedTraversalBuilder : ITraversalBuilder
    {
        private readonly Traversal _traversalSoFar;
        private readonly ImmutableDictionary<InPort, int> _inToOffset;

        public CompletedTraversalBuilder(Traversal traversalSoFar, int inSlots, ImmutableDictionary<InPort, int> inToOffset,
            Attributes attributes)
        {
            _inToOffset = inToOffset;
            _traversalSoFar = traversalSoFar;

            InSlots = inSlots;
            Attributes = attributes;

            Traversal = attributes == Attributes.None
                ? traversalSoFar
                : new PushAttributes(attributes).Concat(traversalSoFar).Concat(PopAttributes.Instance);
        }

        public ITraversalBuilder Add(ITraversalBuilder submodule, Shape shape)
        {
            var key = new BuilderKey();
            return new CompositeTraversalBuilder(
                reverseBuildSteps: new List<ITraversalBuildStep> { key },
                inSlots: InSlots,
                inOffsets: _inToOffset,
                pendingBuilders: ImmutableDictionary<BuilderKey, ITraversalBuilder>.Empty.Add(key, this),
                attributes: Attributes
            );
        }

        public ITraversalBuilder Add<TMat, TMat1, TMat2>(ITraversalBuilder submodule, Shape shape, Func<TMat, TMat1, TMat2> combineMaterialized)
        {
            return Add(submodule, shape);
        }

        public ITraversalBuilder TransformMataterialized<TMat, TMat1>(Func<TMat, TMat1> mapper)
        {
            var transform = new Transform(o => mapper((TMat)o) as object);
            return new CompletedTraversalBuilder(_traversalSoFar.Concat(transform), InSlots, _inToOffset, Attributes);
        }

        public ITraversalBuilder SetAttributes(Attributes attributes) =>
            new CompletedTraversalBuilder(_traversalSoFar, InSlots, _inToOffset, attributes);

        public Attributes Attributes { get; }

        public ITraversalBuilder Wire(OutPort @out, InPort @in) =>
            throw new InvalidOperationException("Cannot wire ports in a completed builder.");

        public int OffsetOfModule(OutPort @out) =>
            throw new InvalidOperationException("Cannot look up offsets in a completed builder.");

        public bool IsUnwired(OutPort @out) => false;

        public bool IsUnwired(InPort @in) => _inToOffset.ContainsKey(@in);

        public int OffsetOf(InPort @in) => _inToOffset[@in];

        public ITraversalBuilder Assign(OutPort @out, int relativeSlot) =>
            throw new InvalidOperationException("Cannot assign ports in a completed builder.");

        public bool IsTraversalComplete { get; } = true;

        public int InSlots { get; }

        public Traversal Traversal { get; }

        public int UnwiredOuts { get; } = 0;

        public ITraversalBuilder MakeIsland(IslandTag islandTag) =>
            new CompletedTraversalBuilder(new EnterIsland(islandTag, _traversalSoFar), InSlots, _inToOffset, Attributes);
    }

    /// <summary>
    /// Represents a builder that contains a single atomic module. Its primary purpose is to track and build the
    /// outToSlot array which will be then embedded in a <see cref="MaterializeAtomic{TShape}"/> Traversal step.
    /// </summary>
    internal sealed class AtomicTraversalBuilder<TShape> : ITraversalBuilder where TShape : Shape
    {
        private readonly IAtomicModule<TShape> _module;
        private readonly int[] _outToSlot;

        public AtomicTraversalBuilder(IAtomicModule<TShape> module, int[] outToSlot, int unwiredOuts, Attributes attributes)
        {
            _module = module;
            _outToSlot = outToSlot;
            UnwiredOuts = unwiredOuts;
            Attributes = attributes;
            InSlots = module.Shape.Inlets.Length;
        }

        public ITraversalBuilder Add<T1, T2, T3>(ITraversalBuilder submodule, Shape shape, Func<T1, T2, T3> combineMaterialized)
        {
            // TODO: Use automatically a linear builder if applicable
            // Create a composite, add ourselves, then the other.
            return new CompositeTraversalBuilder(Attributes)
                .Add<T1, T2, T2>(this, _module.Shape, Keep.Right)
                .Add(submodule, shape, combineMaterialized);
        }

        public ITraversalBuilder TransformMataterialized<T1, T2>(Func<T1, T2> mapper) =>
            TraversalBuilder.Empty()
                .Add(this, _module.Shape)
                .TransformMataterialized(mapper);

        public ITraversalBuilder SetAttributes(Attributes attributes) =>
            new AtomicTraversalBuilder<TShape>(_module, _outToSlot, UnwiredOuts, attributes);

        public Attributes Attributes { get; }

        public ITraversalBuilder Wire(OutPort @out, InPort @in) => Assign(@out, OffsetOf(@in) - OffsetOfModule(@out));

        public int OffsetOfModule(OutPort @out) => 0;

        public bool IsUnwired(OutPort @out) => true;

        public bool IsUnwired(InPort @in) => true;

        public int OffsetOf(InPort @in) => @in.Id;

        public ITraversalBuilder Assign(OutPort @out, int relativeSlot)
        {
            // Create a new array, with the output port assigned to its relative slot
            var newOutToSlot = new int[_outToSlot.Length];
            Array.Copy(_outToSlot, newOutToSlot, _outToSlot.Length);
            newOutToSlot[@out.Id] = relativeSlot;

            // Check if every output port has been assigned, if yes, we have a Traversal for this module.
            var newUnwiredOuts = UnwiredOuts - 1;
            if (newUnwiredOuts == 0)
            {
                return new CompletedTraversalBuilder(
                    traversalSoFar: new MaterializeAtomic<TShape>(_module, newOutToSlot),
                    inSlots: InSlots,
                    // TODO Optimize Map creation
                    inToOffset: _module.Shape.Inlets.ToImmutableDictionary(i => (InPort)i, i => i.Id),
                    attributes: Attributes);
            }

            return new AtomicTraversalBuilder<TShape>(_module, newOutToSlot, newUnwiredOuts, Attributes);
        }

        public bool IsTraversalComplete { get; } = false;

        public int InSlots { get; }

        public Traversal Traversal =>
            throw new IllegalStateException("Traversal can be only acquired from a completed builder");

        public int UnwiredOuts { get; }

        public ITraversalBuilder MakeIsland(IslandTag islandTag) =>
            TraversalBuilder.Empty()
                .Add(this, _module.Shape)
                .MakeIsland(islandTag);
    }

    /// <summary>
    /// Traversal builder that is optimized for linear graphs (those that contain modules with at most one input and
    /// at most one output port). The Traversal is simply built up in reverse order and output ports are automatically
    /// assigned to -1 due to the nature of the graph. The only exception is when composites created by
    /// <see cref="CompositeTraversalBuilder"/> are embedded. These are not guaranteed to have their unwired input/output ports
    /// in a fixed location, therefore the last step of the Traversal might need to be changed in those cases from the
    /// -1 relative offset to something else (see rewireLastOutTo).
    /// </summary>
    public sealed class LinearTraversalBuilder : ITraversalBuilder
    {
        private static readonly LinearTraversalBuilder CachedEmptyLinear =
            new LinearTraversalBuilder(Option<InPort>.None, Option<OutPort>.None, 0, 0, PushNotUsed.Instance,
                Option<ITraversalBuilder>.None, Attributes.None);

        private static readonly int[] WireBackward = { -1 };
        private static readonly int[] NoWire = { };

        public static LinearTraversalBuilder Empty(Attributes attributes = null) =>
            attributes == null || attributes == Attributes.None
                ? CachedEmptyLinear
                : new LinearTraversalBuilder(Option<InPort>.None, Option<OutPort>.None, 0, 0, PushNotUsed.Instance, Option<ITraversalBuilder>.None, attributes);

        /// <summary>
        /// Create a traversal builder specialized for linear graphs. This is designed to be much faster and lightweight
        /// than its generic counterpart. It can be freely mixed with the generic builder in both ways.
        /// </summary>
        public static LinearTraversalBuilder FromModule<TShape>(IAtomicModule<TShape> module, Attributes attributes = null) where TShape : Shape
        {
            if(module.Shape.Inlets.Length > 1)
                throw new ArgumentException("Modules with more than one input port cannot be linear.", nameof(module));

            if (module.Shape.Outlets.Length > 1)
                throw new ArgumentException("Modules with more than one output port cannot be linear.", nameof(module));

            attributes = attributes ?? Attributes.None;

            TraversalBuilder.InitShape(module.Shape);

            var inPort = module.Shape.Inlets.FirstOrDefault();
            var outPort = module.Shape.Outlets.FirstOrDefault();

            var wiring = outPort != null ? WireBackward : NoWire;

            Option<T> FromPort<T>(T port) => port == null ? Option<T>.None : new Option<T>(port);

            return new LinearTraversalBuilder(
                FromPort((InPort)inPort),
                FromPort((OutPort)outPort),
                0, 
                inPort != null ? 1 : 0,
                new MaterializeAtomic<TShape>(module, wiring),
                Option<ITraversalBuilder>.None, 
                attributes);
        }

        public static Traversal AddMaterializeCompose<TMat, TMat1, TAny>(Traversal t,
            Func<TMat, TMat1, TAny> materializeCompose)
        {
            if (Keep.IsLeft(materializeCompose))
                return Pop.Instance.Concat(t);
            if (Keep.IsRight(materializeCompose))
                return t.Concat(Pop.Instance);

            // TODO: Optimize this case so the extra function allocation is not needed. Maybe ReverseCompose?
            var compose = new Compose((second, first) => materializeCompose((TMat)first, (TMat1)second));
            return t.Concat(compose);
        }

        public static LinearTraversalBuilder FromBuilder<T1, T2, T3>(ITraversalBuilder traversalBuilder, Shape shape,
            Func<T1, T2, T3> combine = null)
        {
            if (traversalBuilder is LinearTraversalBuilder linear)
            {
                if (combine == null || Keep.IsRight(combine))
                    return linear;

                return Empty().Append(linear, combine);
            }

            var inlet = shape.Inlets.FirstOrDefault();
            var inOffs = inlet == null ? 0 : traversalBuilder.OffsetOf(inlet);

            if (traversalBuilder is CompletedTraversalBuilder completed)
            {
                return new LinearTraversalBuilder(
                    inlet == null ? Option<InPort>.None : new Option<InPort>(inlet),
                    Option<OutPort>.None,
                    inOffs,
                    completed.InSlots,
                    completed.Traversal.Concat(AddMaterializeCompose(PushNotUsed.Instance, combine)),
                    Option<ITraversalBuilder>.None, 
                    Attributes.None);
            }

            var composite = traversalBuilder;
            var o = shape.Outlets.First(); // Cannot be empty, otherwise it would be a CompletedTraversalBuilder

            return new LinearTraversalBuilder(
                inlet == null ? Option<InPort>.None : new Option<InPort>(inlet),
                new Option<OutPort>(o),
                inOffs,
                composite.InSlots,
                AddMaterializeCompose(PushNotUsed.Instance, combine),
                new Option<ITraversalBuilder>(composite),
                Attributes.None,
                inlet == null ? EmptyTraversal.Instance as Traversal : new PushAttributes(composite.Attributes)
            );
        }

        private readonly Option<InPort> _inPort;
        private readonly Option<OutPort> _outPort;
        private readonly int _inOffset;
        private readonly Traversal _traversalSoFar;
        private readonly Option<ITraversalBuilder> _pendingBuilder;
        private readonly Traversal _beforeBuilder;

        public LinearTraversalBuilder(Option<InPort> inPort, Option<OutPort> outPort, int inOffset, int inSlots,
            Traversal traversalSoFar, Option<ITraversalBuilder> pendingBuilder, Attributes attributes,
            Traversal beforeBuilder = null)
        {
            _inPort = inPort;
            _outPort = outPort;
            _inOffset = inOffset;
            _traversalSoFar = traversalSoFar;
            _pendingBuilder = pendingBuilder;
            _beforeBuilder = beforeBuilder ?? EmptyTraversal.Instance;

            InSlots = inSlots;
            Attributes = attributes;

            Traversal = ApplyAttributes(traversalSoFar);
            IsTraversalComplete = !outPort.HasValue;
            UnwiredOuts = outPort.HasValue ? 1 : 0;
            IsEmpty = InSlots == 0 && !outPort.HasValue;
        }

        public bool IsEmpty { get; }

        public Attributes Attributes { get; }

        public bool IsTraversalComplete { get; } 

        public int InSlots { get; }

        /// <summary>
        /// This builder can always return traversal.
        /// </summary>
        public Traversal Traversal { get; }

        public int UnwiredOuts { get; }

        public ITraversalBuilder Add<TMat, TMat1, TMat2>(ITraversalBuilder submodule, Shape shape, Func<TMat, TMat1, TMat2> combineMaterialized)
        {
            throw new InvalidOperationException("LinearTraversal does not support free-form addition. Add it into a " +
                                                "composite builder instead and add the second module to that.");
        }

        private Traversal ApplyAttributes(Traversal t)
        {
            var withBuilder = _beforeBuilder.Concat(t);
            if (Attributes == Attributes.None)
                return withBuilder;
            return new PushAttributes(Attributes).Concat(withBuilder).Concat(PopAttributes.Instance);
        }

        /// <summary>
        /// In case the default relative wiring of -1 is not applicable (due to for example an embedded composite
        /// <see cref="CompositeTraversalBuilder"/> created traversal) we need to change the mapping for the module we added
        /// last. This method tears down the traversal until it finds that <see cref="MaterializeAtomic{TShape}"/>, changes the mapping,
        /// then rebuilds the Traversal.
        /// </summary>
        private static Traversal RewireLastOutTo(Traversal traversal, int relativeOffset)
        {
            // If, by luck, the offset is the same as it would be in the normal case, no transformation is needed
            if (relativeOffset == -1) return traversal;
            return traversal.RewireFirstTo(relativeOffset);
        }

        public ITraversalBuilder Assign(OutPort @out, int relativeSlot)
        {
            if (_outPort.HasValue && _outPort.Value == @out)
            {
                if (_pendingBuilder.HasValue)
                {
                    return new LinearTraversalBuilder(_inPort, Option<OutPort>.None, _inOffset, InSlots,
                        _pendingBuilder.Value.Assign(@out, relativeSlot)
                            .Traversal.Concat(_traversalSoFar),
                        Option<ITraversalBuilder>.None, Attributes, _beforeBuilder);
                }

                return new LinearTraversalBuilder(_inPort, Option<OutPort>.None, _inOffset, InSlots,
                    RewireLastOutTo(_traversalSoFar, relativeSlot), _pendingBuilder, Attributes, _beforeBuilder);
            }

            throw new ArgumentException($"Port {@out} cannot be accessed in this builder", nameof(@out));
        }

        public bool IsUnwired(OutPort @out) => _outPort.HasValue && _outPort.Value == @out;

        public bool IsUnwired(InPort @in) => _inPort.HasValue && _inPort.Value == @in;

        /// <summary>
        /// Wraps the builder in an island that can be materialized differently, using async boundaries to bridge between islands.
        /// </summary>
        ITraversalBuilder ITraversalBuilder.MakeIsland(IslandTag islandTag) => MakeIsland(islandTag);

        /// <summary>
        /// Wraps the builder in an island that can be materialized differently, using async boundaries to bridge between islands.
        /// </summary>
        public LinearTraversalBuilder MakeIsland(IslandTag islandTag)
        {
            return new LinearTraversalBuilder(_inPort, _outPort, _inOffset, InSlots,
                new EnterIsland(islandTag, _traversalSoFar), _pendingBuilder, Attributes,
                _beforeBuilder);
        }

        public int OffsetOf(InPort @in)
        {
            if (_inPort.HasValue && _inPort.Value == @in)
                return _inOffset;

            throw new ArgumentException($"Port {@in} cannot be accessed in this builder", nameof(@in));
        }

        public int OffsetOfModule(OutPort @out)
        {
            if (_outPort.HasValue && _outPort.Value == @out)
                return _pendingBuilder.HasValue ? _pendingBuilder.Value.OffsetOfModule(@out) : 0;

            throw new ArgumentException($"Port {@out} cannot be accessed in this builder", nameof(@out));
        }

        ITraversalBuilder ITraversalBuilder.SetAttributes(Attributes attributes) => SetAttributes(attributes);

        public LinearTraversalBuilder SetAttributes(Attributes attributes) =>
            new LinearTraversalBuilder(_inPort, _outPort, _inOffset, InSlots, _traversalSoFar, _pendingBuilder,
                attributes, _beforeBuilder);

        ITraversalBuilder ITraversalBuilder.TransformMataterialized<TMat, TMat1>(Func<TMat, TMat1> mapper) => TransformMataterialized(mapper);

        public LinearTraversalBuilder TransformMataterialized<TMat, TMat1>(Func<TMat, TMat1> mapper)
        {
            return new LinearTraversalBuilder(_inPort, _outPort, _inOffset, InSlots,
                _traversalSoFar.Concat(new Transform(o => mapper((TMat)o) as object)), _pendingBuilder, Attributes,
                _beforeBuilder);
        }

        /// <summary>
        /// Since this is a linear traversal, this should not be called in most of the cases. The only notable
        /// exception is when a Flow is wired to itself.
        /// </summary>
        public ITraversalBuilder Wire(OutPort @out, InPort @in)
        {
            if (_outPort.HasValue && _outPort.Value == @out &&
                _inPort.HasValue && _inPort.Value == @in)
            {
                if (_pendingBuilder.HasValue)
                    return new LinearTraversalBuilder(Option<InPort>.None, Option<OutPort>.None, _inOffset, InSlots,
                        _pendingBuilder.Value.Assign(@out, _inOffset - _pendingBuilder.Value.OffsetOfModule(@out))
                            .Traversal.Concat(_traversalSoFar), Option<ITraversalBuilder>.None, Attributes,
                        _beforeBuilder);

                return new LinearTraversalBuilder(Option<InPort>.None, Option<OutPort>.None, _inOffset, InSlots,
                    RewireLastOutTo(_traversalSoFar, _inOffset), _pendingBuilder, Attributes, _beforeBuilder);
            }

            throw new ArgumentException($"The ports {@in} and {@out} cannot be accessed in this builder.");
        }

        public LinearTraversalBuilder Append<T1, T2, T3>(ITraversalBuilder toAppend, Shape shape,
            Func<T1, T2, T3> materializeCompose)
        {
            return Append(FromBuilder<T1, T2, T2>(toAppend, shape, Keep.Right), materializeCompose);
        }

        /// <summary>
        /// Append any builder that is linear shaped (have at most one input and at most one output port) to the
        /// end of this graph, connecting the output of the last module to the input of the appended module.
        /// </summary>
        public LinearTraversalBuilder Append<T1, T2, T3>(LinearTraversalBuilder toAppend, Func<T1, T2, T3> materializeCompose)
        {
            if (toAppend.IsEmpty) return this;

            if (IsEmpty)
                return new LinearTraversalBuilder(toAppend._inPort, toAppend._outPort, toAppend._inOffset,
                    toAppend.InSlots,
                    toAppend._traversalSoFar.Concat(AddMaterializeCompose(Traversal, materializeCompose)),
                    toAppend._pendingBuilder, toAppend.Attributes, toAppend._beforeBuilder);


            if (_outPort.HasValue)
            {
                if (!toAppend._inPort.HasValue)
                    throw new ArgumentException("Appended linear module must have an unwired input port " +
                                                "because there is a dangling output.");

                Traversal traversalWithWiringCorrected;

                if (_pendingBuilder.HasValue)
                {
                    var @out = _outPort.Value;
                    traversalWithWiringCorrected = ApplyAttributes(_pendingBuilder.Value
                        .Assign(@out,
                            -_pendingBuilder.Value.OffsetOfModule(@out) - toAppend.InSlots + toAppend._inOffset)
                        .Traversal.Concat(_traversalSoFar));
                }
                else
                {
                    // No need to rewire if input port is at the expected position
                    traversalWithWiringCorrected = toAppend._inOffset == toAppend.InSlots - 1
                        ? Traversal
                        : ApplyAttributes(RewireLastOutTo(_traversalSoFar, toAppend._inOffset - toAppend.InSlots));
                }

                var newTraversal = !toAppend._pendingBuilder.HasValue
                    ? toAppend.Traversal.Concat(AddMaterializeCompose(traversalWithWiringCorrected, materializeCompose))
                    : toAppend._traversalSoFar
                        .Concat(PopAttributes.Instance)
                        .Concat(AddMaterializeCompose(traversalWithWiringCorrected, materializeCompose));

                // Simply just take the new unwired ports, increase the number of inSlots and concatenate the traversals
                return new LinearTraversalBuilder(
                    _inPort,
                    toAppend._outPort,
                    _inOffset + toAppend._inOffset,
                    InSlots + toAppend.InSlots,
                    // Build in reverse so it yields a more efficient layout for left-to-right building
                    newTraversal,
                    toAppend._pendingBuilder,
                    Attributes.None,
                    toAppend._pendingBuilder.HasValue
                        ? new PushAttributes(toAppend.Attributes)
                        : EmptyTraversal.Instance as Traversal
                );
            }

            throw new Exception("should this happen?");
        }
    }

    interface ITraversalBuildStep { }

    /// <summary>
    /// Helper class that is only used to identify a <see cref="ITraversalBuilder"/> in a <see cref="CompositeTraversalBuilder"/>. The
    /// reason why this is needed is because the builder is referenced at various places, while it needs to be mutated.
    /// In an immutable data structure this is best done with an indirection, i.e. places refer to this immutable key and
    /// look up the current state in an extra Map.
    /// </summary>
    sealed class BuilderKey : ITraversalBuildStep
    {
        public override string ToString() => "K:" + GetHashCode();
    }

    sealed class AppendTraversal : ITraversalBuildStep
    {
        public Traversal Traversal { get; }

        public AppendTraversal(Traversal traversal) => Traversal = traversal;
    }

    /// <summary>
    /// A generic builder that builds a traversal for graphs of arbitrary shape. The memory retained by this class
    /// usually decreases as ports are wired since auxiliary data is only maintained for ports that are unwired.
    /// 
    /// This builder MUST construct its Traversal in the *exact* same order as its modules were added, since the first
    /// (and subsequent) input port of a module is implicitly assigned by its traversal order. Emitting Traversal nodes
    /// in a non-deterministic order (depending on wiring order) would mess up all relative addressing. This is the
    /// primary technical reason why a reverseTraversal list is maintained and the Traversal can only be completed once
    /// all output ports have been wired.
    /// </summary>
    internal sealed class CompositeTraversalBuilder : ITraversalBuilder
    {
        private readonly Traversal _finalSteps;
        private readonly IReadOnlyList<ITraversalBuildStep> _reverseBuildSteps;
        private readonly ImmutableDictionary<InPort, int> _inOffsets;
        private readonly ImmutableDictionary<OutPort, int> _inBaseOffsetForOut;
        private readonly ImmutableDictionary<BuilderKey, ITraversalBuilder> _pendingBuilders;
        private readonly ImmutableDictionary<OutPort, BuilderKey> _outOwners;
        private readonly IslandTag? _islandTag;

        /// <summary>
        /// A generic builder that builds a traversal for graphs of arbitrary shape. The memory retained by this class
        /// usually decreases as ports are wired since auxiliary data is only maintained for ports that are unwired.
        /// 
        /// This builder MUST construct its Traversal in the *exact* same order as its modules were added, since the first
        /// (and subsequent) input port of a module is implicitly assigned by its traversal order. Emitting Traversal nodes
        /// in a non-deterministic order (depending on wiring order) would mess up all relative addressing. This is the
        /// primary technical reason why a reverseTraversal list is maintained and the Traversal can only be completed once
        /// all output ports have been wired.
        /// </summary>
        /// <param name="reverseBuildSteps"> 
        ///     Keeps track of traversal steps that needs to be concatenated. This is basically
        ///     a "queue" of BuilderKeys that point to builders of submodules/subgraphs. Since it is
        ///     unknown in which order will the submodules "complete" (have all of their outputs assigned)
        ///     we delay the creation of the actual Traversal.
        /// </param>
        /// <param name="inSlots">The number of input ports this graph has in total.</param>
        /// <param name="inOffsets">Map to look up the offset of input ports not yet wired</param>
        /// <param name="inBaseOffsetForOut">Map to look up the base (input) offset of a module that owns the given output port</param>
        /// <param name="pendingBuilders">Map to contain the "mutable" builders referred by BuilderKeys</param>
        /// <param name="outOwners">Map of output ports to their parent builders (actually the BuilderKey)</param>
        /// <param name="unwiredOuts">Number of output ports that have not yet been wired/assigned</param>
        public CompositeTraversalBuilder(
            Attributes attributes,
            Traversal finalSteps = null,
            IReadOnlyList<ITraversalBuildStep> reverseBuildSteps = null,
            int inSlots = 0,
            ImmutableDictionary<InPort, int> inOffsets = null,
            ImmutableDictionary<OutPort, int> inBaseOffsetForOut = null,
            ImmutableDictionary<BuilderKey, ITraversalBuilder> pendingBuilders = null,
            ImmutableDictionary<OutPort, BuilderKey> outOwners = null,
            int unwiredOuts = 0,
            IslandTag? islandTag = null
            )
        {
            _finalSteps = finalSteps ?? EmptyTraversal.Instance;
            _reverseBuildSteps = reverseBuildSteps ?? new List<ITraversalBuildStep>{ new AppendTraversal(PushNotUsed.Instance) };
            _inOffsets = inOffsets ?? ImmutableDictionary<InPort, int>.Empty;
            _inBaseOffsetForOut = inBaseOffsetForOut ?? ImmutableDictionary<OutPort, int>.Empty;
            _pendingBuilders = pendingBuilders ?? ImmutableDictionary<BuilderKey, ITraversalBuilder>.Empty;
            _outOwners = outOwners ?? ImmutableDictionary<OutPort, BuilderKey>.Empty;
            _islandTag = islandTag;

            InSlots = inSlots;
            UnwiredOuts = unwiredOuts;
            Attributes = attributes;
        }

        /// <summary>
        /// Requires that a remapped Shape's ports contain the same ID as their target ports!
        /// </summary>
        public ITraversalBuilder Add<TMat, TMat1, TMat2>(ITraversalBuilder submodule, Shape shape, Func<TMat, TMat1, TMat2> combineMaterialized)
        {
            var builderKey = new BuilderKey();

            var newBuildSteps = new List<ITraversalBuildStep>();

            void CombineSteps(params ITraversalBuildStep[] steps)
            {
                newBuildSteps.AddRange(steps);
                newBuildSteps.AddRange(_reverseBuildSteps);
            }

            if (Keep.IsLeft(combineMaterialized))
                CombineSteps(new AppendTraversal(Pop.Instance), builderKey);
            else if (Keep.IsRight(combineMaterialized))
                CombineSteps(builderKey, new AppendTraversal(Pop.Instance));
            else if (Keep.IsNone(combineMaterialized))
                CombineSteps(
                    new AppendTraversal(Pop.Instance),
                    new AppendTraversal(Pop.Instance),
                    builderKey);
            else
                CombineSteps(
                    new AppendTraversal(new Compose((o, o1) => combineMaterialized((TMat)o, (TMat1)o1))),
                    builderKey);

            CompositeTraversalBuilder added;
            if (submodule.IsTraversalComplete)
            {
                // We only need to keep track of the offsets of unwired inputs. Outputs have all been wired
                // (isTraversalComplete = true).
                var newInOffsets = _inOffsets;
                foreach (var inlet in shape.Inlets)
                {
                    // Calculate offset in the current scope. This is the our first unused input slot plus
                    // the relative offset of the input port in the submodule.
                    // TODO Optimize Map access
                    newInOffsets = newInOffsets.SetItem(inlet, InSlots + submodule.OffsetOf(inlet.MappedTo));
                }

                added = new CompositeTraversalBuilder(Attributes, 
                    _finalSteps,
                    newBuildSteps, 
                    InSlots + submodule.InSlots, 
                    newInOffsets, 
                    _inBaseOffsetForOut, 
                    _pendingBuilders.SetItem(builderKey, submodule), 
                    _outOwners, 
                    UnwiredOuts, 
                    _islandTag);
            }
            else
            {
                // Added module have unwired outputs.
                var newInOffsets = _inOffsets;
                var newBaseOffsetsForOut = _inBaseOffsetForOut;
                var newOutOwners = _outOwners;

                // See the other if case for explanation of this
                foreach (var inlet in shape.Inlets)
                {
                    // Calculate offset in the current scope
                    // TODO Optimize Map access
                    newInOffsets = newInOffsets.SetItem(inlet, InSlots + submodule.OffsetOf(inlet.MappedTo));
                }

                foreach (var outlet in shape.Outlets)
                {
                    // Record the base offsets of all the modules we included and which have unwired output ports. We need
                    // to adjust their offset by inSlots as that would be their new position in this module.
                    newBaseOffsetsForOut =
                        newBaseOffsetsForOut.SetItem(outlet, InSlots + submodule.OffsetOfModule(outlet.MappedTo));
                    // TODO Optimize Map access
                    newOutOwners = newOutOwners.SetItem(outlet, builderKey);
                }

                added = new CompositeTraversalBuilder(
                    Attributes,
                    _finalSteps,
                    newBuildSteps,
                    InSlots+submodule.InSlots,
                    newInOffsets,
                    newBaseOffsetsForOut,
                    _pendingBuilders.SetItem(builderKey, submodule),
                    newOutOwners,
                    UnwiredOuts + submodule.UnwiredOuts,
                    _islandTag);
            }

            return added.CompleteIfPossible();
        }

        public ITraversalBuilder TransformMataterialized<TMat, TMat1>(Func<TMat, TMat1> mapper)
        {
            var finalSteps = _finalSteps.Concat(new Transform(o => mapper((TMat)o)));
            return new CompositeTraversalBuilder(Attributes, finalSteps, _reverseBuildSteps, InSlots, _inOffsets,
                _inBaseOffsetForOut, _pendingBuilders, _outOwners, UnwiredOuts, _islandTag);
        }

        public ITraversalBuilder SetAttributes(Attributes attributes)
        {
            return new CompositeTraversalBuilder(attributes, _finalSteps, _reverseBuildSteps, InSlots, _inOffsets,
                _inBaseOffsetForOut, _pendingBuilders, _outOwners, UnwiredOuts, _islandTag);
        }

        public Attributes Attributes { get; }

        public ITraversalBuilder Wire(OutPort @out, InPort @in)
        {
            var inOffsets = _inOffsets.Remove(@in);
            return new CompositeTraversalBuilder(Attributes, _finalSteps, _reverseBuildSteps, InSlots, inOffsets,
                _inBaseOffsetForOut, _pendingBuilders, _outOwners, UnwiredOuts, _islandTag)
                .Assign(@out, OffsetOf(@in) - OffsetOfModule(@out));
        }

        public int OffsetOfModule(OutPort @out) => _inBaseOffsetForOut[@out];

        public bool IsUnwired(OutPort @out) => _inBaseOffsetForOut.ContainsKey(@out);

        public bool IsUnwired(InPort @in) => _inOffsets.ContainsKey(@in);

        public int OffsetOf(InPort @in) => _inOffsets[@in];

        /// <summary>
        ///  Assign an output port a relative slot (relative to the base input slot of its module, see <see cref="MaterializeAtomic{TShape}"/>)
        /// </summary>
        public ITraversalBuilder Assign(OutPort @out, int relativeSlot)
        {
            // Which module out belongs to (indirection via BuilderKey and pendingBuilders)
            var builderKey = _outOwners[@out];
            var submodule = _pendingBuilders[builderKey];

            // Do the assignment in the submodule
            var result = submodule.Assign(@out.MappedTo, relativeSlot);

            CompositeTraversalBuilder wired;
            if (result.IsTraversalComplete)
            {
                // Remove the builder (and associated data).
                // We can't simply append its Traversal as there might be uncompleted builders that come earlier in the
                // final traversal (remember, input ports are assigned in traversal order of modules, and the inOffsets
                // and inBaseOffseForOut Maps are updated when adding a module; we must respect addition order).

                wired = new CompositeTraversalBuilder(Attributes, _finalSteps, _reverseBuildSteps, InSlots, _inOffsets,
                    inBaseOffsetForOut: _inBaseOffsetForOut.Remove(@out),
                    // TODO Optimize Map access
                    pendingBuilders: _pendingBuilders.SetItem(builderKey, result),
                    outOwners: _outOwners.Remove(@out),
                    unwiredOuts: UnwiredOuts - 1,
                    islandTag: _islandTag);
            }
            else
            {
                // Update structures with result
                wired = new CompositeTraversalBuilder(Attributes, _finalSteps, _reverseBuildSteps, InSlots, _inOffsets,
                    inBaseOffsetForOut: _inBaseOffsetForOut.Remove(@out),
                    // TODO Optimize Map access
                    pendingBuilders: _pendingBuilders.SetItem(builderKey, result),
                    outOwners: _outOwners,
                    unwiredOuts: UnwiredOuts - 1,
                    islandTag: _islandTag);
            }

            // If we have no more unconnected outputs, we can finally build the Traversal and shed most of the auxiliary data.
            return wired.CompleteIfPossible();
        }

        private ITraversalBuilder CompleteIfPossible()
        {
            if (UnwiredOuts != 0) return this;

            var traversal = _finalSteps;
            var remaining = _reverseBuildSteps;

            foreach (var step in remaining)
            {
                if (step is BuilderKey key)
                    // At this point all the builders we have are completed and we can finally build our traversal
                    traversal = _pendingBuilders[key].Traversal.Concat(traversal);
                else if (step is AppendTraversal append)
                    traversal = append.Traversal.Concat(traversal);
                else
                    throw new Exception("Should not happen");
            }

            var finalTraversal = !_islandTag.HasValue ? traversal : new EnterIsland(_islandTag.Value, traversal);

            // The CompleteTraversalBuilder only keeps the minimum amount of necessary information that is needed for it
            // to be embedded in a larger graph, making partial graph reuse much more efficient.
            return new CompletedTraversalBuilder(
                finalTraversal, 
                InSlots,
                _inOffsets, 
                Attributes);
        }

        public bool IsTraversalComplete { get; } = false;

        public int InSlots { get; }

        public Traversal Traversal =>
            throw new IllegalStateException("Traversal can be only acquired from a completed builder");

        public int UnwiredOuts { get; }

        public ITraversalBuilder MakeIsland(IslandTag islandTag)
        {
            return new CompositeTraversalBuilder(Attributes, _finalSteps, _reverseBuildSteps, InSlots, _inOffsets,
                    _inBaseOffsetForOut, _pendingBuilders, _outOwners, UnwiredOuts, islandTag);
        }

        public override string ToString()
        {
            string PrintList<T>(IEnumerable<T> list) => $"[{string.Join(", ", list)}]";

            string PrintDic<TKey, TValue>(IDictionary<TKey, TValue> dic)
            {
                var pairs = dic.Select(pair => $"{{ {pair.Key}, {pair.Value} }}");
                return PrintList(pairs);
            }

            return "CompositeTraversal(" +
                   $"    reverseTraversal = [{PrintList(_reverseBuildSteps)}]" +
                   $"    pendingBuilders = {PrintDic(_pendingBuilders)}" +
                   $"    inSlots = {InSlots}" +
                   $"    inOffsets = {PrintDic(_inOffsets)}" +
                   $"    inBaseOffsetForOut = {PrintDic(_inBaseOffsetForOut)}" +
                   $"    outOwners = {PrintDic(_outOwners)}" +
                   $"    unwiredOuts = {UnwiredOuts}" +
                   ")";
        }
    }

}
