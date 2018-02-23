//-----------------------------------------------------------------------
// <copyright file="ActorMaterializerImpl.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using Akka.Actor;
using Akka.Annotations;
using Akka.Dispatch;
using Akka.Event;
using Akka.Pattern;
using Akka.Streams.Implementation.Fusing;
using Akka.Util;
using Akka.Util.Internal;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// ExtendedActorMaterializer used by subtypes which materializer using GraphInterpreterShell
    /// </summary>
    public abstract class ExtendedActorMaterializer : ActorMaterializer
    {
        /// <summary>
        /// INTERNAL API
        /// </summary>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="runnable">TBD</param>
        /// <param name="subFlowFuser">TBD</param>
        /// <returns>TBD</returns>
        [InternalApi]
        public abstract TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Func<GraphInterpreterShell, IActorRef> subFlowFuser);

        /// <summary>
        /// INTERNAL API
        /// </summary>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="runnable">TBD</param>
        /// <param name="subFlowFuser">TBD</param>
        /// <param name="initialAttributes">TBD</param>
        /// <returns>TBD</returns>
        [InternalApi]
        public abstract TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Func<GraphInterpreterShell, IActorRef> subFlowFuser, Attributes initialAttributes);

        /// <summary>
        /// INTERNAL API
        /// </summary>
        /// <param name="context">TBD</param>
        /// <param name="props">TBD</param>
        /// <returns>TBD</returns>
        [InternalApi]
        public override IActorRef ActorOf(MaterializationContext context, Props props)
        {
            var dispatcher = props.Deploy.Dispatcher == Deploy.NoDispatcherGiven
                ? EffectiveSettings(context.EffectiveAttributes).Dispatcher
                : props.Dispatcher;

            return ActorOf(props, context.StageName, dispatcher);
        }

        /// <summary>
        /// INTERNAL API
        /// </summary>
        /// <param name="props">TBD</param>
        /// <param name="name">TBD</param>
        /// <param name="dispatcher">TBD</param>
        /// <exception cref="IllegalStateException">TBD</exception>
        /// <returns>TBD</returns>
        // TODO: hide it again
        [InternalApi]
        public IActorRef ActorOf(Props props, string name, string dispatcher)
        {
            if (Supervisor is LocalActorRef localActorRef)
                return ((ActorCell) localActorRef.Underlying).AttachChild(props.WithDispatcher(dispatcher),
                    isSystemService: false, name: name);


            if (Supervisor is RepointableActorRef repointableActorRef)
            {
                if (repointableActorRef.IsStarted)
                    return ((ActorCell)repointableActorRef.Underlying).AttachChild(props.WithDispatcher(dispatcher), isSystemService: false, name: name);

                var timeout = repointableActorRef.Underlying.System.Settings.CreationTimeout;
                var f = repointableActorRef.Ask<IActorRef>(new StreamSupervisor.Materialize(props.WithDispatcher(dispatcher), name), timeout);
                return f.Result;
            }

            throw new IllegalStateException($"Stream supervisor must be a local actor, was [{Supervisor.GetType()}]");
        }
    }


    /// <summary>
    /// TBD
    /// </summary>
    public class SubFusingActorMaterializerImpl : IMaterializer
    {
        private readonly ExtendedActorMaterializer _delegateMaterializer;
        private readonly Func<GraphInterpreterShell, IActorRef> _registerShell;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="delegateMaterializer">TBD</param>
        /// <param name="registerShell">TBD</param>
        public SubFusingActorMaterializerImpl(ExtendedActorMaterializer delegateMaterializer, Func<GraphInterpreterShell, IActorRef> registerShell)
        {
            _delegateMaterializer = delegateMaterializer;
            _registerShell = registerShell;
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="namePrefix">TBD</param>
        /// <returns>TBD</returns>
        public IMaterializer WithNamePrefix(string namePrefix)
            => new SubFusingActorMaterializerImpl((PhasedFusingActorMaterializerImpl) _delegateMaterializer.WithNamePrefix(namePrefix), _registerShell);

        /// <summary>
        /// TBD
        /// </summary>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="runnable">TBD</param>
        /// <returns>TBD</returns>
        public TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable)
            => _delegateMaterializer.Materialize(runnable, _registerShell);

        /// <summary>
        /// TBD
        /// </summary>
        /// <typeparam name="TMat">TBD</typeparam>
        /// <param name="runnable">TBD</param>
        /// <param name="initialAttributes">TBD</param>
        /// <returns>TBD</returns>
        public TMat Materialize<TMat>(IGraph<ClosedShape, TMat> runnable, Attributes initialAttributes) =>
            _delegateMaterializer.Materialize(runnable, _registerShell, initialAttributes);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="delay">TBD</param>
        /// <param name="action">TBD</param>
        /// <returns>TBD</returns>
        public ICancelable ScheduleOnce(TimeSpan delay, Action action)
            => _delegateMaterializer.ScheduleOnce(delay, action);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="initialDelay">TBD</param>
        /// <param name="interval">TBD</param>
        /// <param name="action">TBD</param>
        /// <returns>TBD</returns>
        public ICancelable ScheduleRepeatedly(TimeSpan initialDelay, TimeSpan interval, Action action)
            => _delegateMaterializer.ScheduleRepeatedly(initialDelay, interval, action);

        /// <summary>
        /// TBD
        /// </summary>
        public MessageDispatcher ExecutionContext => _delegateMaterializer.ExecutionContext;
    }

    /// <summary>
    /// TBD
    /// </summary>
    public class FlowNameCounter : ExtensionIdProvider<FlowNameCounter>, IExtension
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="system">TBD</param>
        /// <returns>TBD</returns>
        public static FlowNameCounter Instance(ActorSystem system)
            => system.WithExtension<FlowNameCounter, FlowNameCounter>();

        /// <summary>
        /// TBD
        /// </summary>
        public readonly AtomicCounterLong Counter = new AtomicCounterLong(0);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="system">TBD</param>
        /// <returns>TBD</returns>
        public override FlowNameCounter CreateExtension(ExtendedActorSystem system) => new FlowNameCounter();
    }

    /// <summary>
    /// TBD
    /// </summary>
    public class StreamSupervisor : ActorBase
    {
        #region Messages

        /// <summary>
        /// TBD
        /// </summary>
        public sealed class Materialize : INoSerializationVerificationNeeded, IDeadLetterSuppression
        {
            /// <summary>
            /// TBD
            /// </summary>
            public readonly Props Props;

            /// <summary>
            /// TBD
            /// </summary>
            public readonly string Name;

            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="props">TBD</param>
            /// <param name="name">TBD</param>
            public Materialize(Props props, string name)
            {
                Props = props;
                Name = name;
            }
        }
        /// <summary>
        /// TBD
        /// </summary>
        public sealed class GetChildren
        {
            /// <summary>
            /// TBD
            /// </summary>
            public static readonly GetChildren Instance = new GetChildren();
            private GetChildren() { }
        }
        /// <summary>
        /// TBD
        /// </summary>
        public sealed class StopChildren
        {
            /// <summary>
            /// TBD
            /// </summary>
            public static readonly StopChildren Instance = new StopChildren();
            private StopChildren() { }
        }
        /// <summary>
        /// TBD
        /// </summary>
        public sealed class StoppedChildren
        {
            /// <summary>
            /// TBD
            /// </summary>
            public static readonly StoppedChildren Instance = new StoppedChildren();
            private StoppedChildren() { }
        }
        /// <summary>
        /// TBD
        /// </summary>
        public sealed class PrintDebugDump
        {
            /// <summary>
            /// TBD
            /// </summary>
            public static readonly PrintDebugDump Instance = new PrintDebugDump();
            private PrintDebugDump() { }
        }
        /// <summary>
        /// TBD
        /// </summary>
        public sealed class Children
        {
            /// <summary>
            /// TBD
            /// </summary>
            public readonly IImmutableSet<IActorRef> Refs;
            /// <summary>
            /// TBD
            /// </summary>
            /// <param name="refs">TBD</param>
            public Children(IImmutableSet<IActorRef> refs)
            {
                Refs = refs;
            }
        }

        #endregion

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="settings">TBD</param>
        /// <param name="haveShutdown">TBD</param>
        /// <returns>TBD</returns>
        public static Props Props(ActorMaterializerSettings settings, AtomicBoolean haveShutdown)
            => Actor.Props.Create(() => new StreamSupervisor(settings, haveShutdown)).WithDeploy(Deploy.Local);

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public static string NextName() => ActorName.Next();

        private static readonly EnumerableActorName ActorName = new EnumerableActorNameImpl("StreamSupervisor", new AtomicCounterLong(0L));

        /// <summary>
        /// TBD
        /// </summary>
        public readonly ActorMaterializerSettings Settings;
        /// <summary>
        /// TBD
        /// </summary>
        public readonly AtomicBoolean HaveShutdown;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="settings">TBD</param>
        /// <param name="haveShutdown">TBD</param>
        public StreamSupervisor(ActorMaterializerSettings settings, AtomicBoolean haveShutdown)
        {
            Settings = settings;
            HaveShutdown = haveShutdown;
        }

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        protected override SupervisorStrategy SupervisorStrategy() => Actor.SupervisorStrategy.StoppingStrategy;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="message">TBD</param>
        /// <returns>TBD</returns>
        protected override bool Receive(object message)
        {
            if (message is Materialize)
            {
                var materialize = (Materialize) message;
                Sender.Tell(Context.ActorOf(materialize.Props, materialize.Name));
            }
            else if (message is GetChildren)
                Sender.Tell(new Children(Context.GetChildren().ToImmutableHashSet()));
            else if (message is StopChildren)
            {
                foreach (var child in Context.GetChildren())
                    Context.Stop(child);

                Sender.Tell(StoppedChildren.Instance);
            }
            else
                return false;
            return true;
        }

        /// <summary>
        /// TBD
        /// </summary>
        protected override void PostStop() => HaveShutdown.Value = true;
    }
}