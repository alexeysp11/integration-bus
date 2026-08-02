using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.SagaOrchestrator.Service.DbContexts;

/// <summary>
/// Represents the database context handling state persistence for the transaction saga 
/// and managing transactional outbox/inbox capabilities via MassTransit.
/// </summary>
/// <param name="options">The database context options to be used by this context.</param>
public sealed class SagaDbContext(DbContextOptions<SagaDbContext> options) : DbContext(options)
{
    /// <summary>
    /// Configures the relational database schema, mapping the saga state instances 
    /// and MassTransit transactional outbox components to PostgreSQL tables.
    /// </summary>
    /// <param name="modelBuilder">The builder being used to construct the model for this context.</param>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Apply the saga class map configuration directly as a standard EF Core configuration
        modelBuilder.ApplyConfiguration(new TransactionSagaMap());

        // Configure MassTransit infrastructure tables directly on the model builder surface
        modelBuilder.AddTransactionalOutboxEntities();
    }
}
