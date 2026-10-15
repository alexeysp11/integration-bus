using System.Data.Common;
using Dapper;

namespace IntegrationBus.AccountBalance.Service.Providers;

/// <summary>
/// Serializes journal appends with PostgreSQL transaction-scoped advisory locks so concurrent consumers on any replica
/// cannot read the same maximum sequence number or double-post one transaction.
/// </summary>
/// <remarks>
/// Design and failure semantics: <c>docs/chaos-engineering/README.md</c> (relative to the repository root).
/// </remarks>
public static class AccountAdvisoryLock
{
    private const string AcquireSql = "SELECT pg_advisory_xact_lock(hashtextextended(@Key, 0));";

    /// <summary>
    /// Acquires the lock of one transaction identifier; it is released automatically when the database transaction ends.
    /// </summary>
    /// <param name="connection">The open connection that owns the transaction.</param>
    /// <param name="transaction">The active database transaction.</param>
    /// <param name="transactionId">The business transaction whose journal entries must be posted exactly once.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be canceled.</param>
    public static Task AcquireTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        CancellationToken cancellationToken) =>
        connection.ExecuteAsync(new CommandDefinition(
            AcquireSql, new { Key = $"tx:{transactionId}" }, transaction, cancellationToken: cancellationToken));

    /// <summary>
    /// Acquires the locks of one or more accounts in ascending identifier order, which prevents lock-order deadlocks.
    /// </summary>
    /// <param name="connection">The open connection that owns the transaction.</param>
    /// <param name="transaction">The active database transaction.</param>
    /// <param name="accountIds">The accounts whose journal streams are about to be appended.</param>
    /// <param name="cancellationToken">Propagates notification that the operation should be canceled.</param>
    public static async Task AcquireAccountsAsync(
        DbConnection connection,
        DbTransaction transaction,
        IEnumerable<Guid> accountIds,
        CancellationToken cancellationToken)
    {
        foreach (Guid accountId in accountIds.Distinct().Order())
        {
            await connection.ExecuteAsync(new CommandDefinition(
                AcquireSql, new { Key = $"account:{accountId}" }, transaction, cancellationToken: cancellationToken));
        }
    }
}
