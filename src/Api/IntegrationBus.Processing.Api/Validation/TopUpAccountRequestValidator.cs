using FluentValidation;
using IntegrationBus.Contracts.Enums;
using IntegrationBus.Contracts.Http;

namespace IntegrationBus.Processing.Api.Validation;

/// <summary>
/// Defines inbound structural and business validation rules for the account replenishment request contract.
/// </summary>
public sealed class TopUpAccountRequestValidator : AbstractValidator<TopUpAccountRequest>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TopUpAccountRequestValidator"/> class and configures rules for properties.
    /// </summary>
    public TopUpAccountRequestValidator()
    {
        RuleFor(x => x.Amount)
            .GreaterThan(0.00m)
            .WithMessage("Top-up amount must be strictly greater than zero.")
            .Must(HaveValidFinancialPrecision)
            .WithMessage("Top-up amount precision cannot exceed 4 decimal places.");

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
