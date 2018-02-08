//-----------------------------------------------------------------------
// <copyright file="CoupledTerminationFlowSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------
using System;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.TestKit;
using FluentAssertions;
using Reactive.Streams;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Tests.Dsl
{
    public class CoupledTerminationFlowSpec : AkkaSpec
    {
        private const string Success = "Success";
        private const string Canceled = "cancel-received";
        private const string Failure = "Boom";

        private ActorMaterializer Materializer { get; }

        public CoupledTerminationFlowSpec(ITestOutputHelper helper) : base(helper)
        {
            var settings = ActorMaterializerSettings.Create(Sys).WithInputBuffer(2, 16);
            Materializer = ActorMaterializer.Create(Sys, settings);
        }

        /// <summary>
        /// The table of effects is exactly the same one as in the docs of the class,
        /// so we're able to directly copy that table to check we wrote correct docs.
        /// </summary>
        [Theory]
        [InlineData("flow cause: upstream (sink-side) receives completion", "sink effect: receives completion", "source effect: receives cancel")]
        [InlineData("flow cause: upstream (sink-side) receives error", "sink effect: receives error", "source effect: receives cancel")]
        [InlineData("flow cause: downstream (source-side) receives cancel", "sink effect: completes", "source effect: receives cancel")]
        [InlineData("flow effect: cancels upstream, completes downstream", "sink effect: completes", "source cause: signals complete")]
        [InlineData("flow effect: cancels upstream, errors downstream", "sink effect: receives error", "source cause: signals error or throws")]
        [InlineData("flow effect: cancels upstream, completes downstream", "sink cause: cancels", "source effect: receives cancel")]
        public void Completion_must_work_for_effects_table(string outerRule, string innerSinkRule, string innerSourceRule)
        {
            var t1 = InterpretOuter(outerRule);
            var outerSource = t1.Item1;
            var outerSink = t1.Item2;
            var outerAssertions = t1.Item3;
            var t2 = InterpretInnerSink(innerSinkRule);
            var innerSink = t2.Item1;
            var innerSinkAssertion = t2.Item2;
            var t3 = InterpretInnerSource(innerSourceRule);
            var innerSource = t3.Item1;
            var innerSourceAssertion = t3.Item2;

            var flow = CoupledTerminationFlow.FromSinkAndSource(innerSink, innerSource);
            outerSource.Via(flow).To(outerSink).Run(Materializer);

            outerAssertions();
            innerSinkAssertion();
            innerSourceAssertion();
        }

        [Fact]
        public void Completion_must_complete_out_source_and_then_complete_in_sink()
        {
            var probe = CreateTestProbe();
            var f = CoupledTerminationFlow.FromSinkAndSource(
                Sink.OnComplete<string>(() => probe.Tell("done"), _ => probe.Tell("done")), 
                Source.Empty<string>());// completes right away, should complete the sink as well

            f.RunWith(Source.Maybe<string>(), Sink.Ignore<string>(), Materializer); // these do nothing.

            probe.ExpectMsg("done");
        }

        [Fact]
        public void Completion_must_cancel_in_sink_and_then_cancel_out_source()
        {
            var probe = CreateTestProbe();
            var f = CoupledTerminationFlow.FromSinkAndSource(
                Sink.Cancelled<string>(),
                Source.FromPublisher(new CancelledPublisher(probe.Ref)));
            // completes right away, should complete the sink as well

            f.RunWith(Source.Maybe<string>(), Sink.Ignore<string>(), Materializer); // these do nothing.

            probe.ExpectMsg(Canceled);
        }

        [Fact]
        public void Completion_must_error_wrapped_sink_when_wrapped_source_errors()
        {
            var probe = CreateTestProbe();
            var f = CoupledTerminationFlow.FromSinkAndSource(
                Sink.OnComplete<string>(() => probe.Tell("done"), e => probe.Tell(e.Message)),
                Source.Failed<string>(new Exception(Failure)));// completes right away, should complete the sink as well

            f.RunWith(Source.Maybe<string>(), Sink.Ignore<string>(), Materializer); // these do nothing.

            probe.ExpectMsg(Failure);
        }

        private Tuple<Source<string, NotUsed>, Sink<string, NotUsed>, Action> InterpretOuter(string rule)
        {
            const string success = "Success";
            const string canceled = "cancel-received";
            const string failure = "Boom";

            var probe = CreateTestProbe();
            var causeUpstreamCompletes = Source.Empty<string>();
            var causeUpstreamErrors = Source.Failed<string>(new Exception(failure));
            var causeDownstreamCancels = Sink.Cancelled<string>();

            var downstreamEffect = Sink.OnComplete<string>(
                () => probe.TestActor.Tell(success),
                ex => probe.TestActor.Tell(ex.Message));
            var upstreamEffect = Source.FromPublisher(new CancelledPublisher(probe.Ref));

            void AssertCancel() => probe.ExpectMsg<string>().Should().Be(canceled);
            void AssertComplete() => probe.ExpectMsg<string>().Should().Be(success);
            void AssertError() => probe.ExpectMsg<string>().Should().Be(failure);

            void AssertCompleteAndCancel()
            {
                probe.ExpectMsgAnyOf(success, canceled);
                probe.ExpectMsgAnyOf(success, canceled);
            }


            void AssertErrorAndCancel()
            {
                probe.ExpectMsgAnyOf(failure, canceled);
                probe.ExpectMsgAnyOf(failure, canceled);
            }

            if (rule.Contains("cause"))
            {
                if (rule.Contains("upstream") && (rule.Contains("completion") || rule.Contains("complete")))
                    return Tuple.Create(causeUpstreamCompletes, downstreamEffect, (Action)AssertComplete);
                if (rule.Contains("upstream") && rule.Contains("error"))
                    return Tuple.Create(causeUpstreamErrors, downstreamEffect, (Action)AssertError);
                if (rule.Contains("downstream") && rule.Contains("cancel"))
                    return Tuple.Create(upstreamEffect, causeDownstreamCancels, (Action)AssertCancel);
                throw new UnableToInterpretRule(rule);
            }

            if (rule.Contains("effect"))
            {
                if (rule.Contains("cancel") && rule.Contains("complete"))
                    return Tuple.Create(upstreamEffect, downstreamEffect, (Action)AssertCompleteAndCancel);
                if (rule.Contains("cancel") && rule.Contains("error"))
                    return Tuple.Create(upstreamEffect, downstreamEffect, (Action)AssertErrorAndCancel);
                throw new UnableToInterpretRule(rule);
            }

            throw new UnableToInterpretRule(rule);
        }

        private sealed class CancelledPublisher : IPublisher<string>
        {
            private sealed class Subscription : ISubscription
            {
                private readonly IActorRef _testActor;

                public Subscription(IActorRef testActor) => _testActor = testActor;

                public void Request(long n) { }

                public void Cancel() => _testActor.Tell(Canceled);
            }

            private readonly IActorRef _testActor;

            public CancelledPublisher(IActorRef testActor) => _testActor = testActor;

            public void Subscribe(ISubscriber<string> subscriber) => subscriber.OnSubscribe(new Subscription(_testActor));
        }

        private Tuple<Sink<string, NotUsed>, Action> InterpretInnerSink(string rule)
        {
            var probe = CreateTestProbe();
            var causeCancel = Sink.Cancelled<string>();

            var catchEffect = Sink.OnComplete<string>(
                () => probe.TestActor.Tell(Success),
                ex => probe.TestActor.Tell(ex.Message));

            void AssertComplete() => probe.ExpectMsg<string>().Should().Be(Success);
            void AssertError() => probe.ExpectMsg<string>().Should().Be(Failure);
            void AssertionOk() => 1.Should().Be(1);

            if (rule.Contains("cause"))
            {
                if (rule.Contains("cancels"))
                    return Tuple.Create(causeCancel, (Action)AssertionOk);
                throw new UnableToInterpretRule(rule);
            }

            if (rule.Contains("effect"))
            {
                if (rule.Contains("complete") || rule.Contains("completion"))
                    return Tuple.Create(catchEffect, (Action)AssertComplete);
                if (rule.Contains("error"))
                    return Tuple.Create(catchEffect, (Action)AssertError);

                throw new UnableToInterpretRule(rule);
            }

            throw new UnableToInterpretRule(rule);
        }
        
        private Tuple<Source<string, NotUsed>, Action> InterpretInnerSource(string rule)
        {
            var probe = CreateTestProbe();
            var causeComplete = Source.Empty<string>();
            var causeError = Source.Failed<string>(new Exception(Failure));

            var catchEffect = Source.Maybe<string>().MapMaterializedValue(c =>
            {
                c.Task.ContinueWith(t =>
                {
                    if (t.IsCanceled || t.IsFaulted)
                        probe.Ref.Tell(t.Exception.Message);
                    else if(t.Result != null) probe.Ref.Tell(Success);
                    else probe.Ref.Tell(Canceled);
                });
                return NotUsed.Instance;
            });
            
            void AssertCancel() => probe.ExpectMsg<string>().Should().Be(Canceled);
            void AssertionOk() => 1.Should().Be(1);

            if (rule.Contains("cause"))
            {
                if (rule.Contains("complete"))
                    return Tuple.Create(causeComplete, (Action)AssertionOk);
                if (rule.Contains("error"))
                    return Tuple.Create(causeError, (Action)AssertionOk);
                throw new UnableToInterpretRule(rule);
            }

            if (rule.Contains("effect"))
                return Tuple.Create(catchEffect, (Action)AssertCancel);

            throw new UnableToInterpretRule(rule);
        }

        private sealed class UnableToInterpretRule : Exception
        {
            public UnableToInterpretRule(string msg) : base("Unable to interpret rule: " + msg)
            {
            }
        }
    }
}
