# InfluxDB Connector (Go)

### Descrição
Este serviço é o componente de persistência temporal da **Mauá SmartFarm**. Ele atua como um consumidor do barramento de eventos (**RabbitMQ**), transformando payloads JSON em pontos de dados otimizados para o **InfluxDB Cloud**.

### Bibliotecas e Justificativa
- **`influxdb-client-go/v2`**: Driver oficial que suporta escrita otimizada e gerenciamento de lotes (batching), essencial para reduzir o número de requisições HTTP para a nuvem.
- **`amqp091-go`**: Garante a comunicação estável com o RabbitMQ, permitindo o desacoplamento do sistema.

### Justificativa para o TCC (ODS 9 e 13)
Diferente de bancos NoSQL genéricos, o InfluxDB é uma base de dados de séries temporais. Isso permite que a plataforma execute **análises preditivas** e cálculos de média móvel com baixíssimo custo computacional, auxiliando o produtor no enfrentamento de mudanças climáticas extremas.