using System;
using System.Diagnostics;
using System.Threading;

namespace MessageQueuePerfTest
{
    public interface IMessageQueue : IDisposable
    {
        void StartConsuming();
        void Publish(string text);
    }

    internal class Program
    {
        public const int TIMES = 1000;

        private static readonly AutoResetEvent _evt = new AutoResetEvent(false);
        private static int _received;

        public static void Increment()
        {
            if (Interlocked.Increment(ref _received) == TIMES)
                _evt.Set();
        }

        private static void Main(string[] args)
        {
            //RunTest("rabbitmq", new RabbitMq(true));
            //RunTest("activemq", new ActiveMq(true));
            RunTest("msmq", new Msmq(true));
        }

        private static void RunTest(string desc, IMessageQueue queue)
        {
            _received = 0;
            _evt.Reset();
            using (queue)
            {
                queue.StartConsuming();

                var sw = Stopwatch.StartNew();
                for (int i = 0; i < TIMES; i++)
                {
                    queue.Publish(i.ToString());
                }

                _evt.WaitOne();
                Console.WriteLine(desc + ": " + sw.Elapsed);
            }
        }
    }
}