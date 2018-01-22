//-----------------------------------------------------------------------
// <copyright file="TransformProcessorTest.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Stage;
using Reactive.Streams;

namespace Akka.Streams.Tests.TCK
{
    class TransformProcessorTest : AkkaIdentityProcessorVerification<int?>
    {
        private sealed class Stage : SimpleLinearGraphStage<int?>
        {
            private sealed class Logic : InAndOutGraphStageLogic
            {
                private readonly Stage _stage;

                public Logic(Stage stage) : base(stage.Shape)
                {
                    _stage = stage;
                    SetHandler(_stage.Inlet, stage.Outlet, this);
                }

                public override void OnPush() => Push(_stage.Outlet, Grab(_stage.Inlet));

                public override void OnPull() => Pull(_stage.Inlet);

            }

            protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
        }

        public override int? CreateElement(int element) => element;

        public override IProcessor<int?,int?> CreateIdentityProcessor(int bufferSize)
        {
            var settings = ActorMaterializerSettings.Create(System).WithInputBuffer(bufferSize/2, bufferSize);
            var materializer = ActorMaterializer.Create(System, settings);

            return Flow.Create<int?>().Via(new Stage()).ToProcessor().Run(materializer);
        }
    }
}
