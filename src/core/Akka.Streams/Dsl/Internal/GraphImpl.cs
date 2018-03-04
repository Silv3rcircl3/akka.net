//-----------------------------------------------------------------------
// <copyright file="GraphImpl.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Annotations;
using Akka.Streams.Implementation;

namespace Akka.Streams.Dsl.Internal
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    /// <typeparam name="TShape">TBD</typeparam>
    /// <typeparam name="TMat">TBD</typeparam>
    [InternalApi]
    public class GraphImpl<TShape, TMat> : IGraph<TShape, TMat> where TShape : Shape
    {
        public GraphImpl(TShape shape, ITraversalBuilder builder)
        {
            Shape = shape;
            Builder = builder;
        }

        /// <summary>
        /// TBD
        /// </summary>
        public TShape Shape { get; }

        public ITraversalBuilder Builder { get; }

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="attributes">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<TShape, TMat> WithAttributes(Attributes attributes) => new GraphImpl<TShape, TMat>(Shape, Builder.SetAttributes(attributes));

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="attributes">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<TShape, TMat> AddAttributes(Attributes attributes) => WithAttributes(Builder.Attributes.And(attributes));

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="name">TBD</param>
        /// <returns>TBD</returns>
        public IGraph<TShape, TMat> Named(string name) => AddAttributes(Attributes.CreateName(name));

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public IGraph<TShape, TMat> Async() => AddAttributes(new Attributes(Attributes.AsyncBoundary.Instance));

        /// <summary>
        /// TBD
        /// </summary>
        /// <returns>TBD</returns>
        public override string ToString() => $"Graph({Shape})";
    }
}