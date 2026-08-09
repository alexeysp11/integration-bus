using IntegrationBus.Contracts.Http;
using IntegrationBus.Processing.Api.Validation;
using FluentValidation.TestHelper;
using IntegrationBus.Contracts.Enums;
using FluentAssertions;

namespace IntegrationBus.Processing.Api.Tests.Validation;

public sealed class StartTransactionRequestValidatorTests
{
    private readonly StartTransactionRequestValidator _validator = new();

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
        StartTransactionRequest request = new()
        {
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 500.00m,
            Currency = validCurrency
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveValidationErrorFor(x => x.Currency);
    }

    [Theory(DisplayName = "Validator passes for valid financial precision edge cases")]
    [InlineData(100)]         // Integer value with no decimal places
    [InlineData(100.1)]       // One decimal place
    [InlineData(100.12)]      // Two decimal places
    [InlineData(100.123)]     // Three decimal places
    [InlineData(100.1234)]    // Exactly four decimal places (maximum valid boundary)
    public void Validate_ShouldPass_WhenAmountHasValidPrecisionEdgeCases(decimal validPrecisionAmount)
    {
        // Arrange
        StartTransactionRequest request = new() { Amount = validPrecisionAmount };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveValidationErrorFor(x => x.Amount);
    }

    [Fact(DisplayName = "Validator fails when financial precision is minimally invalid at the fifth decimal place")]
    public void Validate_ShouldFail_WhenAmountIsMinimallyInvalidAtFifthDecimalPlace()
    {
        // Arrange
        // 0.00001 is strictly invalid as it represents the first step beyond 4 decimal places
        StartTransactionRequest request = new() { Amount = 0.00001m };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Transaction amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator handles decimal maximum value safely without throwing overflow exceptions")]
    public void Validate_ShouldHandleMaximumDecimalValue_WithoutOverflowRuntimeFailures()
    {
        // Arrange
        // Verifies a hidden edge case where multiplying by 10000m inside the precision check could cause an arithmetic overflow
        StartTransactionRequest request = new() { Amount = decimal.MaxValue };

        // Act
        Action validateAction = () => _validator.TestValidate(request);

        // Assert
        // The validator must process the internal logic safely and register a failure instead of crashing the pipeline
        validateAction.Should().NotThrow<OverflowException>();
    }

    [Fact(DisplayName = "Validator passes when all transaction request properties are perfectly valid")]
    public void Validate_ShouldPass_WhenRequestIsValid()
    {
        // Arrange
        StartTransactionRequest request = new()
        {
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Amount = 150.75m,
            Currency = Currency.USD
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Fact(DisplayName = "Validator fails when identifiers are empty Guids")]
    public void Validate_ShouldFail_WhenIdentifiersAreEmpty()
    {
        // Arrange
        StartTransactionRequest request = new()
        {
            SourceAccountId = Guid.Empty,
            TargetAccountId = Guid.Empty
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.SourceAccountId)
            .WithErrorMessage("Source account identifier is required and cannot be empty.");

        result.ShouldHaveValidationErrorFor(x => x.TargetAccountId)
            .WithErrorMessage("Target account identifier is required and cannot be empty.");
    }

    [Fact(DisplayName = "Validator fails when target account identifier matches the source account identifier")]
    public void Validate_ShouldFail_WhenTargetAccountMatchesSourceAccount()
    {
        // Arrange
        Guid identicalAccountId = Guid.NewGuid();
        StartTransactionRequest request = new()
        {
            SourceAccountId = identicalAccountId,
            TargetAccountId = identicalAccountId
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.TargetAccountId)
            .WithErrorMessage("Target account identifier cannot match the source account identifier.");
    }

    [Theory(DisplayName = "Validator fails when transfer amount is zero or negative")]
    [InlineData(0.00)]
    [InlineData(-10.50)]
    public void Validate_ShouldFail_WhenAmountIsZeroOrNegative(decimal invalidAmount)
    {
        // Arrange
        StartTransactionRequest request = new() { Amount = invalidAmount };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Transaction amount must be a positive value strictly greater than zero.");
    }

    [Fact(DisplayName = "Validator passes when financial volume precision is exactly 4 decimal places")]
    public void Validate_ShouldPass_WhenAmountHasExactlyFourDecimalPlaces()
    {
        // Arrange
        StartTransactionRequest request = new() { Amount = 100.1234m };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldNotHaveValidationErrorFor(x => x.Amount);
    }

    [Fact(DisplayName = "Validator fails when financial volume precision exceeds 4 decimal places")]
    public void Validate_ShouldFail_WhenAmountExceedsFourDecimalPlaces()
    {
        // Arrange
        StartTransactionRequest request = new() { Amount = 100.12345m };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Transaction amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator fails when currency property is explicitly set to None")]
    public void Validate_ShouldFail_WhenCurrencyIsNone()
    {
        // Arrange
        StartTransactionRequest request = new() { Currency = Currency.None };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Currency)
            .WithErrorMessage("Currency must be explicitly specified and cannot be set to None.");
    }

    [Fact(DisplayName = "Validator fails when currency value is completely undefined in the enum scope")]
    public void Validate_ShouldFail_WhenCurrencyValueIsMalformed()
    {
        // Arrange
        // Cast an undefined integer to simulate a corrupted or unmapped external payload value
        Currency malformedCurrency = (Currency)999;
        StartTransactionRequest request = new() { Currency = malformedCurrency };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Currency)
            .WithErrorMessage("Provided currency identifier is malformed or completely unsupported.");
    }

    [Fact(DisplayName = "Validator successfully catches invalid precision on a valid high-magnitude decimal with 5 decimal places")]
    public void Validate_ShouldFail_WhenHighMagnitudeAmountExceedsFourDecimalPlaces()
    {
        // Arrange
        // This is the true maximum possible boundary for a decimal to hold exactly 5 decimal places without compiler auto-rounding
        decimal validScaleFiveAmount = 7922816251426433759354.03351m;

        StartTransactionRequest request = new()
        {
            SourceAccountId = Guid.NewGuid(),
            TargetAccountId = Guid.NewGuid(),
            Currency = Currency.USD,
            Amount = validScaleFiveAmount
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Transaction amount precision cannot exceed 4 decimal places.");
    }

    [Fact(DisplayName = "Validator successfully catches invalid precision on subnormal or extremely small fractional decimal values")]
    public void Validate_ShouldFail_WhenAmountHasExtremelyDeepPrecisionScale()
    {
        // Arrange
        // 1e-28m is the absolute minimum positive non-zero value for a .NET decimal type
        decimal microscopicAmount = 0.0000000000000000000000000001m;
        StartTransactionRequest request = new()
        {
            Amount = microscopicAmount
        };

        // Act
        TestValidationResult<StartTransactionRequest> result = _validator.TestValidate(request);

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.Amount)
            .WithErrorMessage("Transaction amount precision cannot exceed 4 decimal places.");
    }
}
