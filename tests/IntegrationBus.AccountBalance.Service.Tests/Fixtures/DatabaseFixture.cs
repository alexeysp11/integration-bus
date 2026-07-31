using Testcontainers.PostgreSql;

namespace IntegrationBus.AccountBalance.Service.Tests.Fixtures;

public sealed class DatabaseFixture : IAsyncLifetime
{
    public PostgreSqlContainer Container { get; } = new PostgreSqlBuilder("postgres")
        .WithDatabase("accounting_test")
        .WithUsername("postgres")
        .WithPassword("postgres")
        .Build();

    public async Task InitializeAsync() => await Container.StartAsync();

    public async Task DisposeAsync() => await Container.DisposeAsync();
}
