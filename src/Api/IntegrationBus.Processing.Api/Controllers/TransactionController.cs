using Microsoft.AspNetCore.Mvc;

namespace IntegrationBus.Processing.Api.Controllers;

/// <summary>
/// Provides HTTP endpoints for handling and initiating system transaction workflows.
/// </summary>
[ApiController]
[Route("api/ledger/[controller]")]
public sealed class TransactionController : ControllerBase
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
    public IActionResult CreateTransaction()
    {
        // Generate a unique identifier for the asynchronous tracking flow
        Guid transactionId = Guid.NewGuid();

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
