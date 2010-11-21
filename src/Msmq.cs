using System;
using System.Messaging;

namespace MessageQueuePerfTest
{
    public class Msmq : IMessageQueue, IDisposable
    {
        private const string QueueName = @".\private$\test_queue";

        static Msmq()
        {
            if (!MessageQueue.Exists(QueueName))
                MessageQueue.Create(QueueName, true);
        }

        private readonly MessageQueue _queue = new MessageQueue(QueueName);

        public Msmq(bool durable)
        {
            _queue.DefaultPropertiesToSend.Recoverable = durable;
        }

        void IMessageQueue.StartConsuming()
        {
            _queue.Purge();
            _queue.ReceiveCompleted += OnMessageReceived;
            _queue.BeginReceive();
        }

        void IMessageQueue.Publish(string text)
        {
            _queue.Send(text, MessageQueueTransactionType.Single);
        }

        private void OnMessageReceived(object sender, ReceiveCompletedEventArgs e)
        {
            _queue.EndReceive(e.AsyncResult);
            _queue.BeginReceive();
            Program.Increment();
        }

        public void Dispose()
        {
            _queue.Close();
            GC.SuppressFinalize(this);
        }
    }
}