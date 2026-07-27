using IntegrationBus.Contracts.Http;
using IntegrationBus.Processing.Api.Validation;
using FluentValidation.TestHelper;
using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.Processing.Api.Tests.Validation;

public sealed class TopUpAccountRequestValidatorTests
{
    private readonly TopUpAccountRequestValidator _validator = new();

    [Theory(DisplayName = "Validator passes for every explicitly supported active currency type")]
    [InlineData(Currency.USD)]
    [InlineData(Currency.EUR)]
    [InlineData(Currency.CHF)]
    [InlineData(Currency.AUD)]
    [InlineData(Currency.CAD)]
    [InlineData(Currency.AED)]
    [InlineData(Currency.GEL)]
    [InlineData(Currency.JPY)]
    [InlineData(Currency.CNY)]
    [InlineData(Currency.RUB)]
    [InlineData(Currency.BYN)]
    public void Validate_ShouldPass_ForAllSupportedActiveCurrencies(Currency validCurrency)
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = 100.50m,
            Currency = validCurrency
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Theory(DisplayName = "Validator fails when top up amount is zero or negative")]
    [InlineData(0.00)]
    [InlineData(-50.25)]
    public void Validate_ShouldFail_WhenAmountIsZeroOrNegative(decimal invalidAmount)
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = invalidAmount,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Top-up amount must be strictly greater than zero.");
    }

    [Theory(DisplayName = "Validator passes for valid financial precision edge cases up to 4 decimal places")]
    [InlineData(50)]
    [InlineData(50.1)]
    [InlineData(50.12)]
    [InlineData(50.123)]
    [InlineData(50.1234)]
    public void Validate_ShouldPass_WhenAmountHasValidPrecisionEdgeCases(decimal validPrecisionAmount)
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = validPrecisionAmount,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveValidationErrorFor(x => x.Amount);
    }

    [Fact(DisplayName = "Validator fails when financial precision is minimally invalid at the fifth decimal place")]
    public void Validate_ShouldFail_WhenAmountIsMinimallyInvalidAtFifthDecimalPlace()
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = 0.00001m,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Top-up amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator handles decimal maximum value safely without throwing overflow exceptions")]
    public void Validate_ShouldHandleMaximumDecimalValue_WithoutOverflowRuntimeFailures()
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = decimal.MaxValue,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        // Since scale of decimal.MaxValue is 0 (which is <= 4), the validation rule for precision passes
        result.ShouldNotHaveValidationErrorFor(x => x.Amount);
    }

    [Fact(DisplayName = "Validator successfully catches invalid precision on a valid high magnitude decimal with 5 decimal places")]
    public void Validate_ShouldFail_WhenHighMagnitudeAmountExceedsFourDecimalPlaces()
    {
        // Arrange
        decimal validScaleFiveAmount = 7922816251426433759354.03351m;
        TopUpAccountRequest request = new()
        {
            Amount = validScaleFiveAmount,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Top-up amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator successfully catches invalid precision on subnormal or extremely small fractional decimal values")]
    public void Validate_ShouldFail_WhenAmountHasExtremelyDeepPrecisionScale()
    {
        // Arrange
        decimal microscopicAmount = 0.0000000000000000000000000001m;
        TopUpAccountRequest request = new()
        {
            Amount = microscopicAmount,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Top-up amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator fails when currency property is explicitly set to None")]
    public void Validate_ShouldFail_WhenCurrencyIsNone()
    {
        // Arrange
        TopUpAccountRequest request = new()
        {
            Amount = 100.00m,
            Currency = Currency.None
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Currency)
            .WithErrorMessage("Currency must be explicitly specified and cannot be set to None.");
    }

    [Fact(DisplayName = "Validator fails when currency value is completely undefined in the enum scope")]
    public void Validate_ShouldFail_WhenCurrencyValueIsMalformed()
    {
        // Arrange
        Currency malformedCurrency = (Currency)999;
        TopUpAccountRequest request = new()
        {
            Amount = 100.00m,
            Currency = malformedCurrency
        };

        // Act
        TestValidationResult<TopUpAccountRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Currency)
            .WithErrorMessage("Provided currency identifier is malformed or completely unsupported.");
    }
}
