using System.Data.Common;

namespace IntegrationBus.AccountBalance.Service.Providers;

/// <summary>
/// Defines the architectural boundary contract required to reconstruct the real-time state of an account from its event ledger stream.
/// </summary>
public interface IAccountStateReconstructor
{
    /// <summary>
    /// Computes the absolute real-time liquid balance capacity by aggregating the latest snapshot with subsequent stream logs.
    /// </summary>
    /// <param name="accountId">The unique identifier of the target financial account.</param>
    /// <param name="connection">The active open database connection instance.</param>
    /// <param name="transaction">The active database transaction context boundary.</param>
    /// <param name="cancellationToken">The inbound cancellation token tracking execution session lifetimes.</param>
    Task<decimal> ReconstructAvailableBalanceAsync(
        Guid accountId, 
        DbConnection connection, 
        DbTransaction transaction, 
        CancellationToken cancellationToken = default);
}
