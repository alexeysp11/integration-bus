using FluentValidation;
using IntegrationBus.Contracts.Enums;
using IntegrationBus.Contracts.Http;

namespace IntegrationBus.Processing.Api.Validation;

/// <summary>
/// Defines inbound structural and cross-field validation rules for initiating a new distributed transaction saga.
/// </summary>
public sealed class StartTransactionRequestValidator : AbstractValidator<StartTransactionRequest>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StartTransactionRequestValidator"/> class and configures rules for properties.
    /// </summary>
    public StartTransactionRequestValidator()
    {
        RuleFor(x => x.TransactionId)
            .NotEmpty()
            .WithMessage("TransactionId is required and must be a valid, non-empty unique identifier.");

        RuleFor(x => x.SourceAccountId)
            .NotEmpty()
            .WithMessage("Source account identifier is required and cannot be empty.");

        RuleFor(x => x.TargetAccountId)
            .NotEmpty()
            .WithMessage("Target account identifier is required and cannot be empty.")
            // Enforce cross-field validation to block transfers within the same account
            .NotEqual(x => x.SourceAccountId)
            .WithMessage("Target account identifier cannot match the source account identifier.");

        RuleFor(x => x.Amount)
            .GreaterThan(0.00m)
            .WithMessage("Transaction amount must be a positive value strictly greater than zero.")
            .Must(HaveValidFinancialPrecision)
            .WithMessage("Transaction amount precision cannot exceed 4 decimal places.");

        RuleFor(x => x.Currency)
            .IsInEnum()
            .WithMessage("Provided currency identifier is malformed or completely unsupported.")
            .NotEqual(Currency.None)
            .WithMessage("Currency must be explicitly specified and cannot be set to None.");
    }

    /// <summary>
    /// Verifies that the monetary value does not contain fractional components beyond 4 decimal places.
    /// </summary>
    private static bool HaveValidFinancialPrecision(decimal amount)
        => amount.Scale <= 4;
}
