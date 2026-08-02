using IntegrationBus.SagaOrchestrator.Service.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace IntegrationBus.SagaOrchestrator.Service.DbContexts;

/// <summary>
/// Declares explicit property lengths, constraints, and column mappings for the <see cref="TransactionSagaInstance"/>.
/// </summary>
public sealed class TransactionSagaMap : IEntityTypeConfiguration<TransactionSagaInstance>
{
    /// <summary>
    /// Defines properties configuration and column optimization boundaries for the database mapping engine.
    /// </summary>
    /// <param name="builder">The builder to be used to configure the saga entity.</param>
    public void Configure(EntityTypeBuilder<TransactionSagaInstance> builder)
    {
        // Define explicit table mapping for PostgreSQL
        builder.ToTable("TransactionState");

        // Establish the primary correlation identifier key constraint
        builder.HasKey(x => x.CorrelationId);

        // Enforce the primary correlation identifier constraint syntax
        builder.Property(x => x.CorrelationId)
            .ValueGeneratedNever();

        // Enforce string length validation boundary for the current orchestrator state execution name
        builder.Property(x => x.CurrentState)
            .HasMaxLength(64)
            .IsRequired();

        // Optimize row version concurrency token to prevent race conditions during distributed execution steps
        builder.Property(x => x.RowVersion)
            .IsRowVersion();
    }
}
