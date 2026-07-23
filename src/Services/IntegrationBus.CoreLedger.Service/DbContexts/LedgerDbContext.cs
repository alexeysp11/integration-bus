using IntegrationBus.CoreLedger.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.CoreLedger.Service.DbContexts;

public sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<LedgerEntryEntity> LedgerEntries {  get; set; }
}
