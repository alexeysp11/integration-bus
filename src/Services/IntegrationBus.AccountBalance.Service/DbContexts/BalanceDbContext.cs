using IntegrationBus.AccountBalance.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.AccountBalance.Service.DbContexts;

public sealed class BalanceDbContext(DbContextOptions<BalanceDbContext> options) : DbContext(options)
{
    public DbSet<AccountEntity> Accounts { get; set; }
    public DbSet<AccountHoldEntity> AccountHolds { get; set; }

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
