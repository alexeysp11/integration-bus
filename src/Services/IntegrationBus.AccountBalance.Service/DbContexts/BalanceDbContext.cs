using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.DbContexts;

/// <summary>
/// Provides the updated event-sourced database context boundary for executing immutable ledger transactions.
/// </summary>
public sealed class BalanceDbContext(DbContextOptions<BalanceDbContext> options) : DbContext(options)
{
    /// <summary>
    /// Gets or sets the target database repository collection holding core financial account identity references.
    /// </summary>
    public DbSet<AccountEntity> Accounts { get; set; }

    /// <summary>
    /// Gets or sets the append-only ledger transaction journal holding immutable financial modification history events.
    /// </summary>
    public DbSet<AccountJournalEntryEntity> JournalEntries { get; set; }

    /// <summary>
    /// Gets or sets the periodic historical balance snapshots used to accelerate state reconstruction performance.
    /// </summary>
    public DbSet<AccountSnapshotEntity> Snapshots { get; set; }

    /// <inheritdoc/>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Enforce clustered indexing and performance optimization constraints
        modelBuilder.Entity<AccountJournalEntryEntity>()
            .HasIndex(e => new { e.SourceAccountId, e.SequenceNumber })
            .IsUnique();

        modelBuilder.Entity<AccountSnapshotEntity>()
            .HasIndex(e => new { e.AccountId, e.SequenceNumber })
            .IsUnique();
    }
}
