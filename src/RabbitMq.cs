using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageQueuePerfTest
{
    internal static class RabbitMqExtensions
    {
        public static RabbitSession CreateDurableSession(this IConnection connection)
        {
            return CreateSession(connection, true);
        }

        public static RabbitSession CreateNonDurableSession(this IConnection connection)
        {
            return CreateSession(connection, false);
        }

        public static RabbitSession CreateSession(this IConnection connection, bool durable)
        {
            var s = new RabbitSession { RoutingKey = "rk" + durable, QueueName = "q" + durable, ExchangeName = "ex" + durable };
            var model = connection.CreateModel();
            model.ExchangeDeclare(s.ExchangeName, ExchangeType.Direct, true);
            model.QueueDeclare(s.QueueName, true);
            model.QueueBind(s.QueueName, s.ExchangeName, s.RoutingKey, true, null);
            s.Model = model;
            return s;
        }
    }

    internal class RabbitSession
    {
        public IModel Model;
        public string QueueName;
        public string ExchangeName;
        public string RoutingKey;
    }

    public class RabbitMq : IMessageQueue
    {
        private readonly ConnectionFactory _factory = new ConnectionFactory { HostName = "localhost" };
        private readonly IConnection _connection;
        private bool _running;
        private readonly Thread _consumeThread;
        private readonly RabbitSession _session;
        private readonly IBasicProperties _props;

        public RabbitMq(bool durable)
        {
            _connection = _factory.CreateConnection();
            _session = durable ? _connection.CreateDurableSession() : _connection.CreateNonDurableSession();
            _props = _session.Model.CreateBasicProperties();
            _props.SetPersistent(true);

            _consumeThread = new Thread(ReadMessages);
        }

        void IMessageQueue.StartConsuming()
        {
            _running = true;
            _consumeThread.Start();
        }

        private void ReadMessages()
        {
            var consumer = new QueueingBasicConsumer(_session.Model);
            _session.Model.BasicConsume(_session.QueueName, null, consumer);

            while (consumer.Queue.DequeueNoWait(null) != null) ;

            while (_running)
            {
                object args;
                consumer.Queue.Dequeue(500, out args);

                var e = args as BasicDeliverEventArgs;
                if (e != null)
                {
                    _session.Model.BasicAck(e.DeliveryTag, false);
                    Program.Increment();
                }
            }
        }

        void IMessageQueue.Publish(string text)
        {
            _session.Model.BasicPublish(_session.ExchangeName, _session.RoutingKey, _props, Encoding.Unicode.GetBytes(text));
        }

        public void Dispose()
        {
            if (_running)
            {
                _running = false;
                _consumeThread.Join();
            }
            _session.Model.Dispose();
            _connection.Dispose();
        }
    }
}