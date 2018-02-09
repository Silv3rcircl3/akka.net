//-----------------------------------------------------------------------
// <copyright file="TaskFlattenSourceSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.TestKit;
using Akka.Streams.TestKit.Tests;
using Akka.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Tests.Dsl
{
    public class TaskFlattenSourceSpec : AkkaSpec
    {
        private ActorMaterializer Materializer { get; }

        public TaskFlattenSourceSpec(ITestOutputHelper helper) : base(helper) => Materializer = Sys.Materializer();

        private Source<int, string> Underlying { get; } =
            Source.From(new[] { 1, 2, 3 }).MapMaterializedValue(_ => "foo");

        [Fact]
        public void Task_source_must_emit_the_elements_of_the_task_source() 
            => this.AssertAllStagesStopped(() =>
            {
                var sourceCompletion = new TaskCompletionSource<Source<int, string>>();
                var t = Source.FromTaskSource(sourceCompletion.Task)
                    .ToMaterialized(Sink.Seq<int>(), Keep.Both).Run(Materializer);
                var sourceMaterializedValue = t.Item1;
                var sinkMaterializedValue = t.Item2;

                sourceCompletion.SetResult(Underlying);
                // should complete as soon as inner source has been materialized
                sourceMaterializedValue.AwaitResult().Should().Be("foo");
                sinkMaterializedValue.AwaitResult().Should().BeEquivalentTo(new[] { 1, 2, 3 });
            }, Materializer);


        [Fact]
        public void Task_source_must_handle_downstream_cancelling_before_the_underlying_task_completes()
            => this.AssertAllStagesStopped(() =>
            {
                var sourceCompletion = new TaskCompletionSource<Source<int, string>>();
                var t = Source.FromTaskSource(sourceCompletion.Task)
                    .WatchTermination(Keep.Both)
                    .To(Sink.Cancelled<int>())
                    .Run(Materializer);
                var sourceMaterializedValue = t.Item1;
                var termination = t.Item2;

                // wait for cancellation to occur
                termination.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();

                // even though canceled the underlying matval should arrive
                sourceCompletion.SetResult(Underlying);
                sourceMaterializedValue.AwaitResult().Should().Be("foo");
            }, Materializer);


        [Fact]
        public void Task_source_must_fail_if_the_underlying_task_is_failed()
            => this.AssertAllStagesStopped(() =>
            {
                var failure = new TestException("foo");
                // Task.FromException comes with .Net 4.6, we're still at 4.5
                var underlying = Task.Run(() =>
                {
                    throw failure;
                    return null as Source<int, string>;
                });

                var t = Source.FromTaskSource(underlying)
                    .ToMaterialized(Sink.Seq<int>(), Keep.Both)
                    .Run(Materializer);
                var sourceMaterializedValue = t.Item1;
                var sinkMaterializedValue = t.Item2;

                Action a = () => sourceMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("foo");
                a = () => sinkMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("foo");
            }, Materializer);


        [Fact]
        public void Task_source_must_fail_as_the_underlying_task_fails_after_outer_source_materialization()
            => this.AssertAllStagesStopped(() =>
            {
                var failure = new TestException("foo");
                var sourceCompletion = new TaskCompletionSource<Source<int, string>>();
                var materializationLatch = new TestLatch(1);

                var t = Source.FromTaskSource(sourceCompletion.Task)
                    .MapMaterializedValue(_ =>
                    {
                        materializationLatch.CountDown();
                        return _;
                    })
                    .ToMaterialized(Sink.Seq<int>(), Keep.Both)
                    .Run(Materializer);
                var sourceMaterializedValue = t.Item1;
                var sinkMaterializedValue = t.Item2;

                // we don't know that materialization completed yet (this is still a bit racy)
                materializationLatch.Ready(RemainingOrDefault);
                Thread.Sleep(100);
                sourceCompletion.SetException(failure);

                Action a = () => sourceMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("foo");
                a = () => sinkMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("foo");
            }, Materializer);


        [Fact]
        public void Task_source_must_fail_as_the_underlying_task_fails_after_outer_source_materialization_with_no_demand()
            => this.AssertAllStagesStopped(() =>
            {
                var failure = new TestException("foo");
                var sourceCompletion = new TaskCompletionSource<Source<int, string>>();
                var testProbe = this.CreateSubscriberProbe<int>();

                var sourceMaterializedValue = Source.FromTaskSource(sourceCompletion.Task)
                    .To(Sink.FromSubscriber(testProbe))
                    .Run(Materializer);

                testProbe.ExpectSubscription();
                sourceCompletion.SetException(failure);
                
                Action a = () => sourceMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("foo");
            }, Materializer);


        [Fact]
        public void Task_source_must_handle_backpressure_when_the_task_completes()
            => this.AssertAllStagesStopped(() =>
            {
                var sourceCompletion = new TaskCompletionSource<Source<int, string>>();
                var subscriber = this.CreateSubscriberProbe<int>();
                var publisher = this.CreatePublisherProbe<int>();

                var materializedValue = Source.FromTaskSource(sourceCompletion.Task)
                    .To(Sink.FromSubscriber(subscriber))
                    .Run(Materializer);

                subscriber.EnsureSubscription();

                sourceCompletion.SetResult(Source.FromPublisher(publisher).MapMaterializedValue(_ => "woho"));

                // materialized value completes but still no demand
                materializedValue.AwaitResult().Should().Be("woho");

                // then demand and let an element through to see it works
                subscriber.Request(1);
                publisher.ExpectRequest();
                publisher.SendNext(1);
                subscriber.ExpectNext(1);
                publisher.SendComplete();
                subscriber.ExpectComplete();
            }, Materializer);

        
        [Fact(Skip = "Behaviour when inner source throws during materialization is undefined (leaks ActorGraphInterpreters)" +
                     "until Akka ticket #22358 has been fixed, this test fails because of it")]
        public void Task_source_must_fail_when_the_task_source_materialization_fails()
            => this.AssertAllStagesStopped(() =>
            {
                var failure = new TestException("MatEx");

                var t = Source.FromTaskSource(Task.FromResult(Source.FromGraph(new FailingMaterializationStage())))
                    .ToMaterialized(Sink.Seq<int>(), Keep.Both)
                    .Run(Materializer);

                var sourceMaterializedValue = t.Item1;
                var sinkMaterializedValue = t.Item2;

                Action a = () => sourceMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("MatEx");
                a = () => sinkMaterializedValue.Wait(TimeSpan.FromSeconds(3));
                a.ShouldThrow<TestException>().WithMessage("MatEx");
            }, Materializer);


        private sealed class FailingMaterializationStage : GraphStageWithMaterializedValue<SourceShape<int>, string>
        {
            public FailingMaterializationStage() => Shape = new SourceShape<int>(new Outlet<int>("whatever"));

            public override SourceShape<int> Shape { get; }

            public override ILogicAndMaterializedValue<string> CreateLogicAndMaterializedValue(Attributes inheritedAttributes) => throw new TestException("argh, materialization failed");
        }
    }
}
