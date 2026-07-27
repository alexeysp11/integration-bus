using FluentValidation;
using IntegrationBus.Contracts.Http;

namespace IntegrationBus.Processing.Api.Tests.Extensions.Fakes;

/// <summary>
/// Minimal nested stub for the override test execution scope
/// </summary>
public sealed class ExternalDummyValidator : AbstractValidator<TopUpAccountRequest>;
