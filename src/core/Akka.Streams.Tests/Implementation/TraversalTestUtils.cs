//-----------------------------------------------------------------------
// <copyright file="FixedBufferSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Streams.Implementation;

namespace Akka.Streams.Tests.Implementation
{
    /// <summary>
    /// These test classes do not use the optimized linear builder, for testing the composite builder instead
    /// </summary>
    public static class TraversalTestUtils
    {
        public class CompositeTestSource<T> : AtomicModule
        {
            public CompositeTestSource()
            {
                Shape = new SourceShape<T>(Out);
                Builder = TraversalBuilder.Atomic(this, Attributes.CreateName("testSource"));
            }

            public Outlet<T> Out { get; } =new Outlet<T>("testSourceC.out");

            public override Shape Shape { get; }

            internal ITraversalBuilder Builder { get; }

            public override IModule ReplaceShape(Shape shape)
            {
                throw new NotImplementedException();
            }

            public override IModule CarbonCopy()
            {
                throw new NotImplementedException();
            }

            public override Attributes Attributes { get; }

            public override IModule WithAttributes(Attributes attributes)
            {
                throw new NotImplementedException();
            }

            public override string ToString() => "TestSource";
        }
    }
}
