using IntegrationBus.CoreLedger.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.CoreLedger.Service.DbContexts;

public sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<LedgerEntryEntity> LedgerEntries {  get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // A transaction owns at most one ledger entry; this constraint makes redelivered WriteLedgerRecord commands idempotent
        modelBuilder.Entity<LedgerEntryEntity>()
            .HasIndex(entry => entry.TransactionId)
            .IsUnique();
    }
}
