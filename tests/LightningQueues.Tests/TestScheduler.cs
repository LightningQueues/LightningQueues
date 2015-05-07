using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;

namespace LightningQueues.Tests
{
    /*
    This is here temporarily for dnxcore compatibility, can be removed when compatible nuget is released for TestScheduler
    */

    public struct Recorded<T> : IEquatable<Recorded<T>>
    {
        private readonly long _time;
        private readonly T _value;

        /// <summary>
        /// Gets the virtual time the value was produced on.
        /// </summary>
        public long Time { get { return _time; } }

        /// <summary>
        /// Gets the recorded value.
        /// </summary>
        public T Value { get { return _value; } }

        /// <summary>
        /// Creates a new object recording the production of the specified value at the given virtual time.
        /// </summary>
        /// <param name="time">Virtual time the value was produced on.</param>
        /// <param name="value">Value that was produced.</param>
        public Recorded(long time, T value)
        {
            _time = time;
            _value = value;
        }

        /// <summary>
        /// Checks whether the given recorded object is equal to the current instance.
        /// </summary>
        /// <param name="other">Recorded object to check for equality.</param>
        /// <returns>true if both objects are equal; false otherwise.</returns>
        public bool Equals(Recorded<T> other)
        {
            return Time == other.Time && EqualityComparer<T>.Default.Equals(Value, other.Value);
        }

        /// <summary>
        /// Determines whether the two specified Recorded&lt;T&gt; values have the same Time and Value.
        /// </summary>
        /// <param name="left">The first Recorded&lt;T&gt; value to compare.</param>
        /// <param name="right">The second Recorded&lt;T&gt; value to compare.</param>
        /// <returns>true if the first Recorded&lt;T&gt; value has the same Time and Value as the second Recorded&lt;T&gt; value; otherwise, false.</returns>
        public static bool operator ==(Recorded<T> left, Recorded<T> right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Determines whether the two specified Recorded&lt;T&gt; values don't have the same Time and Value.
        /// </summary>
        /// <param name="left">The first Recorded&lt;T&gt; value to compare.</param>
        /// <param name="right">The second Recorded&lt;T&gt; value to compare.</param>
        /// <returns>true if the first Recorded&lt;T&gt; value has a different Time or Value as the second Recorded&lt;T&gt; value; otherwise, false.</returns>
        public static bool operator !=(Recorded<T> left, Recorded<T> right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Determines whether the specified System.Object is equal to the current Recorded&lt;T&gt; value.
        /// </summary>
        /// <param name="obj">The System.Object to compare with the current Recorded&lt;T&gt; value.</param>
        /// <returns>true if the specified System.Object is equal to the current Recorded&lt;T&gt; value; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            if (obj is Recorded<T>)
                return Equals((Recorded<T>)obj);
            return false;
        }

        /// <summary>
        /// Returns the hash code for the current Recorded&lt;T&gt; value.
        /// </summary>
        /// <returns>A hash code for the current Recorded&lt;T&gt; value.</returns>
        public override int GetHashCode()
        {
            return Time.GetHashCode() + EqualityComparer<T>.Default.GetHashCode(Value);
        }

        /// <summary>
        /// Returns a string representation of the current Recorded&lt;T&gt; value.
        /// </summary>
        /// <returns>String representation of the current Recorded&lt;T&gt; value.</returns>
        public override string ToString()
        {
            return Value.ToString() + "@" + Time.ToString(CultureInfo.CurrentCulture);
        }
    }


    /// <summary>
    /// Observer that records received notification messages and timestamps those.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the sequence.</typeparam>
    public interface ITestableObserver<T> : IObserver<T>
    {
        /// <summary>
        /// Gets recorded timestamped notification messages received by the observer.
        /// </summary>
        IList<Recorded<Notification<T>>> Messages { get; }
    }

    /// <summary>
    /// Virtual time scheduler used for testing applications and libraries built using Reactive Extensions.
    /// </summary>
    public class TestScheduler : VirtualTimeScheduler<long, long>
    {
        /// <summary>
        /// Schedules an action to be executed at the specified virtual time.
        /// </summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="action">Action to be executed.</param>
        /// <param name="dueTime">Absolute virtual time at which to execute the action.</param>
        /// <returns>Disposable object used to cancel the scheduled action (best effort).</returns>
        /// <exception cref="ArgumentNullException"><paramref name="action"/> is null.</exception>
        public override IDisposable ScheduleAbsolute<TState>(TState state, long dueTime, Func<IScheduler, TState, IDisposable> action)
        {
            if (dueTime <= Clock)
                dueTime = Clock + 1;

            return base.ScheduleAbsolute<TState>(state, dueTime, action);
        }

        /// <summary>
        /// Adds a relative virtual time to an absolute virtual time value.
        /// </summary>
        /// <param name="absolute">Absolute virtual time value.</param>
        /// <param name="relative">Relative virtual time value to add.</param>
        /// <returns>Resulting absolute virtual time sum value.</returns>
        protected override long Add(long absolute, long relative)
        {
            return absolute + relative;
        }

        /// <summary>
        /// Converts the absolute virtual time value to a DateTimeOffset value.
        /// </summary>
        /// <param name="absolute">Absolute virtual time value to convert.</param>
        /// <returns>Corresponding DateTimeOffset value.</returns>
        protected override DateTimeOffset ToDateTimeOffset(long absolute)
        {
            return new DateTimeOffset(absolute, TimeSpan.Zero);
        }

        /// <summary>
        /// Converts the TimeSpan value to a relative virtual time value.
        /// </summary>
        /// <param name="timeSpan">TimeSpan value to convert.</param>
        /// <returns>Corresponding relative virtual time value.</returns>
        protected override long ToRelative(TimeSpan timeSpan)
        {
            return timeSpan.Ticks;
        }

        /// <summary>
        /// Starts the test scheduler and uses the specified virtual times to invoke the factory function, subscribe to the resulting sequence, and dispose the subscription.
        /// </summary>
        /// <typeparam name="T">The element type of the observable sequence being tested.</typeparam>
        /// <param name="create">Factory method to create an observable sequence.</param>
        /// <param name="created">Virtual time at which to invoke the factory to create an observable sequence.</param>
        /// <param name="subscribed">Virtual time at which to subscribe to the created observable sequence.</param>
        /// <param name="disposed">Virtual time at which to dispose the subscription.</param>
        /// <returns>Observer with timestamped recordings of notification messages that were received during the virtual time window when the subscription to the source sequence was active.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="create"/> is null.</exception>
        public ITestableObserver<T> Start<T>(Func<IObservable<T>> create, long created, long subscribed, long disposed)
        {
            if (create == null)
                throw new ArgumentNullException("create");

            var source = default(IObservable<T>);
            var subscription = default(IDisposable);
            var observer = CreateObserver<T>();

            ScheduleAbsolute(default(object), created, (scheduler, state) => { source = create(); return Disposable.Empty; });
            ScheduleAbsolute(default(object), subscribed, (scheduler, state) => { subscription = source.Subscribe(observer); return Disposable.Empty; });
            ScheduleAbsolute(default(object), disposed, (scheduler, state) => { subscription.Dispose(); return Disposable.Empty; });

            Start();

            return observer;
        }

        /// <summary>
        /// Creates an observer that records received notification messages and timestamps those.
        /// </summary>
        /// <typeparam name="T">The element type of the observer being created.</typeparam>
        /// <returns>Observer that can be used to assert the timing of received notifications.</returns>
        public ITestableObserver<T> CreateObserver<T>()
        {
            return new MockObserver<T>(this);
        }
    }

    class MockObserver<T> : ITestableObserver<T>
    {
        TestScheduler scheduler;
        List<Recorded<Notification<T>>> messages;

        public MockObserver(TestScheduler scheduler)
        {
            if (scheduler == null)
                throw new ArgumentNullException("scheduler");

            this.scheduler = scheduler;
            this.messages = new List<Recorded<Notification<T>>>();
        }

        public void OnNext(T value)
        {
            messages.Add(new Recorded<Notification<T>>(scheduler.Clock, Notification.CreateOnNext<T>(value)));
        }

        public void OnError(Exception exception)
        {
            messages.Add(new Recorded<Notification<T>>(scheduler.Clock, Notification.CreateOnError<T>(exception)));
        }

        public void OnCompleted()
        {
            messages.Add(new Recorded<Notification<T>>(scheduler.Clock, Notification.CreateOnCompleted<T>()));
        }

        public IList<Recorded<Notification<T>>> Messages
        {
            get { return messages; }
        }
    }
}