//-----------------------------------------------------------------------
// <copyright file="Keep.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Reflection;

namespace Akka.Streams.Dsl
{
    /// <summary>
    /// Convenience functions for often-encountered purposes like keeping only the
    /// left (first) or only the right (second) of two input values.
    /// </summary> 
    public static class Keep
    {
        /// <summary>
        /// Ignores the <paramref name="right"/> value and returns the <paramref name="left"/> value
        /// </summary>
        public static TLeft Left<TLeft, TRight>(TLeft left, TRight right) => left;

        /// <summary>
        /// Ignores the <paramref name="left"/> value and returns the <paramref name="right"/> value
        /// </summary>
        public static TRight Right<TLeft, TRight>(TLeft left, TRight right) => right;

        /// <summary>
        /// Combines <paramref name="left"/> and <paramref name="right"/> into a <see cref="Tuple{T1, T2}"/>
        /// </summary>
        public static Tuple<TLeft, TRight> Both<TLeft, TRight>(TLeft left, TRight right) => Tuple.Create(left, right);

        /// <summary>
        /// Ignores <paramref name="left"/> and <paramref name="right"/> and returns <see cref="NotUsed"/>.
        /// </summary>
        public static NotUsed None<TLeft, TRight>(TLeft left, TRight right) => NotUsed.Instance;

#if !CORECLR
        private static readonly RuntimeMethodHandle KeepRightMethodhandle = typeof(Keep).GetMethod(nameof(Right)).MethodHandle;
#else
        private static readonly MethodInfo KeepRightMethodInfo = typeof(Keep).GetMethod(nameof(Right));
#endif

        /// <summary>
        /// Checks weather the given <paramref name="fn"/> is <see cref="Right{TLeft,TRight}"/>
        /// </summary>
        public static bool IsRight<T1, T2, T3>(Func<T1, T2, T3> fn)
        {
#if !CORECLR
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().MethodHandle.Value == KeepRightMethodhandle.Value;
#else
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().Equals(KeepRightMethodInfo);
#endif
        }

#if !CORECLR
        private static readonly RuntimeMethodHandle KeepLeftMethodhandle = typeof(Keep).GetMethod(nameof(Left)).MethodHandle;
#else
        private static readonly MethodInfo KeepLeftMethodInfo = typeof(Keep).GetMethod(nameof(Left));
#endif


        /// <summary>
        /// Checks weather the given <paramref name="fn"/> is <see cref="Left{TLeft,TRight}"/>
        /// </summary>
        public static bool IsLeft<T1, T2, T3>(Func<T1, T2, T3> fn)
        {
#if !CORECLR
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().MethodHandle.Value == KeepLeftMethodhandle.Value;
#else
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().Equals(KeepLeftMethodInfo);
#endif
        }

#if !CORECLR
        private static readonly RuntimeMethodHandle KeepNoneMethodhandle = typeof(Keep).GetMethod(nameof(None)).MethodHandle;
#else
        private static readonly MethodInfo KeepNoneMethodInfo = typeof(Keep).GetMethod(nameof(None));
#endif


        /// <summary>
        /// Checks weather the given <paramref name="fn"/> is <see cref="None{TLeft,TRight}"/>
        /// </summary>
        public static bool IsNone<T1, T2, T3>(Func<T1, T2, T3> fn)
        {
#if !CORECLR
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().MethodHandle.Value == KeepNoneMethodhandle.Value;
#else
            return fn.GetMethodInfo().IsGenericMethod && fn.GetMethodInfo().GetGenericMethodDefinition().Equals(KeepNoneMethodhandle);
#endif
        }
    }
}