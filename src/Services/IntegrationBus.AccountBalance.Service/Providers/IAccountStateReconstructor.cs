namespace IntegrationBus.AccountBalance.Service.Providers;

/// <summary>
/// Defines the architectural boundary contract required to reconstruct the real-time state of an account from its event ledger stream.
/// </summary>
public interface IAccountStateReconstructor
{
    /// <summary>
    /// Reconstructs the absolute net balance state of a target account by calculating snapshot offsets combined with subsequent ledger streams.
    /// </summary>
    Task<decimal> ReconstructCurrentBalanceAsync(Guid accountId, CancellationToken cancellationToken);
}
