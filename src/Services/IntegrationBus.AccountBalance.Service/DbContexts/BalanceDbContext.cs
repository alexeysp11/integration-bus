using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.DbContexts;

/// <summary>
/// Provides the primary Entity Framework Core database context boundary for executing financial balance transactions and processing resource data retention.
/// </summary>
public sealed class BalanceDbContext(DbContextOptions<BalanceDbContext> options) : DbContext(options)
{
    /// <summary>
    /// Gets or sets the target database repository collection holding active financial account state structures.
    /// </summary>
    public DbSet<AccountEntity> Accounts { get; set; }

    /// <summary>
    /// Gets or sets the target database repository collection holding frozen ledger balance transaction hold reservations.
    /// </summary>
    public DbSet<AccountHoldEntity> AccountHolds { get; set; }

    /// <summary>
    /// Configures the relational schema mapping specifications and registers initial default system seed data payloads.
    /// </summary>
    /// <param name="modelBuilder">The core builder platform configuration engine mapper tool.</param>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<AccountEntity>().HasData(new AccountEntity
        {
            Id = Guid.Parse("a2222222-3333-4444-5555-999999999999"),
            Balance = 50000.00m,
            UpdatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)
        });
    }
}
