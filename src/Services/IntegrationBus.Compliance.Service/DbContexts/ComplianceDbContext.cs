using IntegrationBus.Compliance.Service.Entities;
using Microsoft.EntityFrameworkCore;

namespace IntegrationBus.Compliance.Service.DbContexts;

public sealed class ComplianceDbContext(DbContextOptions<ComplianceDbContext> options) : DbContext(options)
{
    public DbSet<ComplianceAuditEntity> ComplianceAudits {  get; set; }
}
