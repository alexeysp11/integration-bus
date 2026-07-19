using IntegrationBus.Contracts.Http;
using IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;
using MassTransit;
using Microsoft.AspNetCore.Mvc;

namespace IntegrationBus.Processing.Api.Controllers;

/// <summary>
/// Provides HTTP endpoints for handling and initiating system transaction workflows.
/// </summary>
[ApiController]
[Route("api/ledger/[controller]")]
public sealed class TransactionController(
    ILogger<TransactionController> logger,
    ITopicProducer<StartTransactionSaga> producer) : ControllerBase
{
    /// <summary>
    /// Accepts a financial transaction payload and initiates an asynchronous stateful saga execution.
    /// </summary>
    /// <remarks>
    /// This endpoint performs initial payload structural validation. If valid, it immediately dispatches 
    /// a startup command to Apache Kafka and responds to the caller without waiting for the transaction completion.
    /// </remarks>
    /// <response code="202">The transaction payload was successfully validated, accepted, and queued into the execution broker.</response>
    /// <response code="400">The provided transaction model contains structural validation errors or corrupted data fields.</response>
    [HttpPost]
    [EndpointSummary("Create transaction")]
    [EndpointDescription("Accepts a financial transaction payload and initiates an asynchronous stateful saga execution.")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateTransactionAsync([FromBody] StartTransactionRequest request, CancellationToken cancellationToken)
    {
        // Enforce deterministic ID generation if missing from client payload
        Guid transactionId = request.TransactionId == Guid.Empty ? Guid.NewGuid() : request.TransactionId;

        logger.LogInformation(
            "Ingesting transaction request. Allocated TransactionId: {TransactionId} for Amount: {Amount}",
            transactionId,
            request.Amount);

        // Map and publish the initialization command to the dedicated Kafka topic partition
        await producer.Produce(new StartTransactionSaga
        {
            TransactionId = transactionId,
            SourceAccountId = request.SourceAccountId,
            TargetAccountId = request.TargetAccountId,
            Amount = request.Amount,
            Currency = request.Currency
        }, cancellationToken);

        // Prepare the standard enterprise processing stub response
        var response = new
        {
            TransactionId = transactionId,
            Status = "Processing",
            Message = "Your transaction payload has been accepted and queued for processing."
        };

        return Accepted(response);
    }
}
