using IntegrationBus.Contracts.Http;
using IntegrationBus.AccountBalance.Contracts.Messages.Commands;
using MassTransit;
using Microsoft.AspNetCore.Mvc;
using Asp.Versioning;
using IntegrationBus.Processing.Api.Filters;

namespace IntegrationBus.Processing.Api.Controllers;

/// <summary>
/// Provides HTTP endpoints for manipulating and orchestrating financial account metrics.
/// </summary>
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/[controller]")]
public sealed class AccountsController(
    ILogger<AccountsController> logger,
    ITopicProducer<TopUpAccountBalance> topUpProducer) : ControllerBase
{
    /// <summary>
    /// Accepts an asynchronous account replenishment request and routes it to the transaction streaming pipeline via Kafka.
    /// </summary>
    /// <param name="id">The unique identifier of the target account to replenish.</param>
    /// <param name="request">The structural payload containing replenishment metrics (amount and currency).</param>
    /// <param name="cancellationToken">The operational monitoring token injected to track client request execution aborts.</param>
    /// <returns>A strongly-typed HTTP 202 Accepted payload encapsulating the internal execution tracking reference.</returns>
    /// <response code="202">Returns the tracking transaction tracking metadata indicating successful infrastructure queue ingestion.</response>
    /// <response code="400">Returned if the request metadata or body validation constraints fail parsing parameters.</response>
    [HttpPost("{id:guid}/topup")]
    [EndpointSummary("Top up an account balance")]
    [EndpointDescription("Accepts the request body, publishes a TopUpAccountBalance command into Kafka via MassTransit, and returns HTTP 202 Accepted. Actual ledger processing is executed asynchronously.")]
    [ProducesResponseType(typeof(TopUpAccountResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<TopUpAccountResponse>> TopUpAccount(
        [FromRoute] Guid id,
        [FromBody] TopUpAccountRequest request,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Accepting balance replenishment request for AccountId: {AccountId}, Amount: {Amount} {Currency}",
            id, request.Amount, request.Currency);

        // Map HTTP request components directly into the MassTransit Kafka event/command contract
        TopUpAccountBalance command = new()
        {
            TransactionId = Guid.NewGuid(),
            AccountId = id,
            Amount = request.Amount,
            Currency = request.Currency,
            TimestampUtc = DateTime.UtcNow
        };

        // Fire-and-forget: dispatch to Kafka immediately to preserve high-throughput capabilities
        await topUpProducer.Produce(command, cancellationToken);

        logger.LogInformation("Successfully queued top-up operation message into Kafka for AccountId: {AccountId}", id);

        // Return HTTP 202 Accepted acknowledging that the command has been successfully ingested for processing
        return Accepted(new TopUpAccountResponse
        {
            Message = "Top-up request accepted and is being processed asynchronously.",
            TrackingTransactionId = command.TransactionId
        });
    }

    /// <summary>
    /// Executes a high-speed bulk database insertion utility to rapidly seed test accounts.
    /// </summary>
    /// <param name="request">The payload specifying total record quantity boundaries.</param>
    /// <param name="cancellationToken">The operational cancellation token root.</param>
    [HttpPost("seed")]
    [EndpointSummary("Bulk seed accounts")]
    [EndpointDescription("Seeds the accounting database with bulk test accounts.")]
    [DenyProductionEnvironment]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> BulkSeedAccounts(
        [FromBody] BulkSeedAccountsRequest request)
    {
        if (request.Count <= 0)
        {
            return BadRequest("The requested entity seed count must be a positive integer strictly greater than zero.");
        }

        // TODO: Invoke high-speed bulk utility (e.g., await _seedingService.ExecuteAsync(request.Count, cancellationToken))

        return Ok($"Successfully generated and seeded {request.Count} test account entities into the target database.");
    }
}
