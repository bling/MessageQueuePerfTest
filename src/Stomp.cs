using System;
using Apache.NMS;
using Apache.NMS.Stomp;

namespace MessageQueuePerfTest
{
    public class Stomp : IMessageQueue, IDisposable
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly ISession _session;
        private readonly IMessageConsumer _consumer;
        private readonly IMessageProducer _producer;

        public Stomp(bool durable)
        {
            _connectionFactory = new ConnectionFactory("tcp://localhost:61613");
            _connection = _connectionFactory.CreateConnection();
            _connection.ClientId = "13AC0CF8-65FE-4638-8B85-62210DD89BEE";
            _connection.Start();
            _session = _connection.CreateSession();

            var topic = _session.GetQueue("exampleQueue");

            _producer = _session.CreateProducer(topic);
            _producer.DeliveryMode = durable ? MsgDeliveryMode.Persistent : MsgDeliveryMode.NonPersistent;

            _consumer = _session.CreateConsumer(topic);
        }

        void IMessageQueue.StartConsuming()
        {
            while (_consumer.ReceiveNoWait() != null) ;
            _consumer.Listener += _ => Program.Increment();
        }

        void IMessageQueue.Publish(string text)
        {
            _producer.Send(_session.CreateTextMessage(text));
        }

        public void Dispose()
        {
            _connection.Close();
            _session.Close();
            _consumer.Close();
            GC.SuppressFinalize(this);
        }
    }
}