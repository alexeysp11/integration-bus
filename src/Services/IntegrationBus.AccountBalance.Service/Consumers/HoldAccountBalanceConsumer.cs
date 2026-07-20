using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using IntegrationBus.AccountBalance.Contracts.Messages.Events;
using MassTransit;

namespace IntegrationBus.AccountBalance.Service.Consumers;

/// <summary>
/// Handles incoming asset reservation requests from the global Saga Orchestrator.
/// </summary>
public sealed class HoldAccountBalanceConsumer : IConsumer<HoldAccountBalance>
{
    /// <summary>
    /// Executes the baseline funds hold transaction step simulation.
    /// </summary>
    public async Task Consume(ConsumeContext<HoldAccountBalance> context)
    {
        // TODO: In Issue #3, insert record into 'integration_bus_balance' database using SqlConnection/EF.
        // For Issue #2, we log the database interaction and simulate absolute success.
        Console.WriteLine($"[Database: balance] Simulating SQL write: INSERT INTO AccountHolds (TransactionId, AccountId, Amount) VALUES ('{context.Message.TransactionId}', '{context.Message.AccountId}', {context.Message.Amount})");

        // Respond back to the Saga Orchestrator over Kafka mirroring the outcome pattern
        await context.RespondAsync(new HoldAccountBalancePassed
        {
            TransactionId = context.Message.TransactionId,
            HeldAt = DateTime.UtcNow
        });
    }
}
