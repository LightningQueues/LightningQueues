using Xunit;

namespace LightningQueues.Tests
{
    public static class AssertionExtensions
    {
        public static void ShouldEqual<T>(this T expected, T target)
        {
            Assert.Equal(expected, target);
        }

        public static void ShouldBeFalse(this bool value)
        {
            Assert.False(value);
        }

        public static void ShouldBeTrue(this bool value)
        {
            Assert.True(value);
        }

        public static T ShouldBeType<T>(this object value)
        {
            return Assert.IsType<T>(value);
        }

        public static void ShouldNotBeNull<T>(this T value)
        {
            Assert.NotNull(value);
        }

        public static void ShouldBeNull<T>(this T value)
        {
            Assert.Null(value);
        }
    }
}