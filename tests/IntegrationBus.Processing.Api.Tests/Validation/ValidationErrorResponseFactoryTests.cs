using FluentAssertions;
using IntegrationBus.Contracts.Http;
using IntegrationBus.Processing.Api.Validation;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.AspNetCore.Mvc;

namespace IntegrationBus.Processing.Api.Tests.Validation;

public sealed class ValidationErrorResponseFactoryTests
{
    [Fact(DisplayName = "Create transforms populated ModelState into BadRequestObjectResult with the first valid error message")]
    public void Create_ShouldReturnBadRequestWithFirstErrorMessage_WhenModelStateHasValidErrors()
    {
        // Arrange
        ModelStateDictionary modelState = new();
        modelState.AddModelError("Amount", "Amount must be greater than zero.");
        modelState.AddModelError("Currency", "Currency code is invalid.");

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        result.Should().NotBeNull();
        result.StatusCode.Should().Be(400);

        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("Amount must be greater than zero.");
    }

    [Fact(DisplayName = "Create skips empty or whitespace error messages and grabs the first non empty message")]
    public void Create_ShouldSkipWhitespaceErrors_WhenSearchingForFirstErrorMessage()
    {
        // Arrange
        ModelStateDictionary modelState = new();
        modelState.AddModelError("TransactionId", " ");
        modelState.AddModelError("SourceAccountId", "");
        modelState.AddModelError("TargetAccountId", "Target account identifier is required.");

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("Target account identifier is required.");
    }

    [Fact(DisplayName = "Create falls back to default message when ModelState contains absolutely no errors")]
    public void Create_ShouldReturnDefaultFallbackMessage_WhenModelStateIsEmpty()
    {
        // Arrange
        ModelStateDictionary modelState = new();

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("One or more validation errors occurred.");
    }

    [Fact(DisplayName = "Create falls back to default message when ModelState errors contain only null or whitespace values")]
    public void Create_ShouldReturnDefaultFallbackMessage_WhenAllErrorsAreInvalidStrings()
    {
        // Arrange
        ModelStateDictionary modelState = new();
        modelState.AddModelError("Amount", string.Empty);
        modelState.AddModelError("Currency", "   ");

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("One or more validation errors occurred.");
    }

    [Fact(DisplayName = "Create falls back to default message when ModelState contains exception errors without error messages")]
    public void Create_ShouldReturnDefaultFallbackMessage_WhenErrorsContainOnlyExceptions()
    {
        // Arrange
        ModelStateDictionary modelState = new();

        // Leverage the framework's native method to safely initialize the key without manual metadata instantiation
        modelState.MarkFieldSkipped("Amount");

        // Simulate a low-level framework binding failure (e.g., malformed JSON syntax)
        // where ErrorMessage is null but the Exception property is explicitly populated
        Exception bindingException = new FormatException("The input provided was not valid JSON.");
        modelState["Amount"]!.Errors.Add(bindingException);

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("One or more validation errors occurred.");
    }

    [Fact(DisplayName = "Create falls back to default message when ModelState key exists but contains an empty errors collection")]
    public void Create_ShouldReturnDefaultFallbackMessage_WhenErrorsCollectionIsEmpty()
    {
        // Arrange
        ModelStateDictionary modelState = new();

        // Initialize the key entry within the dictionary using framework native APIs
        modelState.MarkFieldSkipped("Amount");

        // Ensure the key exists but explicitly clear any automatically initialized errors 
        // to simulate an empty errors collection edge case
        modelState["Amount"]!.Errors.Clear();

        // Act
        BadRequestObjectResult result = ValidationErrorResponseFactory.Create(modelState);

        // Assert
        ValidationErrorResponse? payload = result.Value as ValidationErrorResponse;
        payload.Should().NotBeNull();
        payload!.Error.Should().Be("One or more validation errors occurred.");
    }

    [Fact(DisplayName = "Create throws NullReferenceException when the provided ModelState dictionary is null")]
    public void Create_ShouldThrowNullReferenceException_WhenModelStateIsNull()
    {
        // Arrange
        ModelStateDictionary? nullModelState = null;

        // Act
        Action act = () => ValidationErrorResponseFactory.Create(nullModelState!);

        // Assert
        // Verified alignment with factory line 20 runtime behavior where modelState.Values is invoked
        act.Should().Throw<NullReferenceException>();
    }
}
