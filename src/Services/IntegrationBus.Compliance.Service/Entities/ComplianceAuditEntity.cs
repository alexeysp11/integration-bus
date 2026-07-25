using IntegrationBus.Compliance.Service.Enums;
using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.Compliance.Service.Entities;

public sealed record ComplianceAuditEntity
{
    public Guid Id { get; set; }
    public Guid TransactionId { get; set; }
    public Guid SourceAccountId { get; set; }
    public Guid TargetAccountId { get; set; }
    public decimal Amount { get; set; }
    public Currency Currency { get; set; } = Currency.None;
    public ComplianceStatus Status { get; set; } = ComplianceStatus.None;
    public string? FailureReason { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
